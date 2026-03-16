package ingres

// Took from https://github.com/ildus/ingres

// export LIBPATH=/opt/freeware/lib:/opt/freeware/lib64:/usr/lib:$COBDIR/lib:$LIBPATH
// export LD_LIBRARY_PATH=/opt/freeware/lib:/opt/freeware/lib64
// export II_SYSTEM=/ingres
// export LD_LIBRARY_PATH=$II_SYSTEM/ingres/lib:$II_SYSTEM/ingres/lib/lp64
// export LIBPATH=$II_SYSTEM/ingres/lib:$II_SYSTEM/ingres/lib/lp64:$LIBPATH
// export PATH=$II_SYSTEM/ingres:$II_SYSTEM/ingres/bin:$II_SYSTEM/ingres/utility:$PATH

/*
#cgo CFLAGS: -I /ingres/ingres/files -pthread -O2
#cgo LDFLAGS: -L/opt/freeware/lib/pthread -L/opt/freeware/lib64 -L/opt/freeware/lib -L/ingres/ingres/lib/lp64 -lingres -lpthread -lrt -lm -lc -lcrypt -ldl -lgcc_s -lstdc++

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <iiapi.h>

IIAPI_INITPARM InitParm = {0, IIAPI_VERSION, 0, NULL};

// golang doesn't support C array, use this to get descriptor item
static inline IIAPI_DESCRIPTOR * get_descr(IIAPI_GETDESCRPARM *descrParm, size_t i)
{
    return &descrParm->gd_descriptor[i];
}

static inline IIAPI_DESCRIPTOR * allocate_descs(short len)
{
    return malloc(sizeof(IIAPI_DESCRIPTOR) * len);
}

static inline IIAPI_DATAVALUE * allocate_cols(short len)
{
    return malloc(sizeof(IIAPI_DATAVALUE) * len);
}

static inline void set_dv_value(IIAPI_DATAVALUE *dest, int i, void *val)
{
    dest[i].dv_value = val;
}

static inline void set_dv_length(IIAPI_DATAVALUE *dest, int i, uint16_t len)
{
    dest[i].dv_length = len;
}

static inline void set_dv_null(IIAPI_DATAVALUE *dest, int i, II_BOOL val)
{
    dest[i].dv_null = val;
}

static inline IIAPI_DESCRIPTOR * get_desc(IIAPI_DESCRIPTOR *dest, uint16_t i)
{
    return &dest[i];
}

static inline IIAPI_DATAVALUE * get_dv(IIAPI_DATAVALUE *dest, uint16_t i)
{
    return &dest[i];
}

static IIAPI_STATUS convert_to_string(IIAPI_DT_ID dt, II_PTR val, uint16_t len,
    II_PTR buf, ushort buflen)
{
    IIAPI_CONVERTPARM	cv;

    memset(buf, 0, buflen);
	cv.cv_srcDesc.ds_dataType = dt;
	cv.cv_srcDesc.ds_nullable = FALSE;
	cv.cv_srcDesc.ds_length = len;
	cv.cv_srcDesc.ds_precision = 0;
	cv.cv_srcDesc.ds_scale = 0;
	cv.cv_srcDesc.ds_columnType = IIAPI_COL_TUPLE;
	cv.cv_srcDesc.ds_columnName = NULL;

	cv.cv_srcValue.dv_null = FALSE;
	cv.cv_srcValue.dv_length = cv.cv_srcDesc.ds_length;
	cv.cv_srcValue.dv_value = val;

	cv.cv_dstDesc.ds_dataType = IIAPI_CHA_TYPE;
	cv.cv_dstDesc.ds_nullable = FALSE;
	cv.cv_dstDesc.ds_length = buflen;
	cv.cv_dstDesc.ds_precision = 0;
	cv.cv_dstDesc.ds_scale = 0;
	cv.cv_dstDesc.ds_columnType = IIAPI_COL_TUPLE;
	cv.cv_dstDesc.ds_columnName = NULL;

	cv.cv_dstValue.dv_null = FALSE;
	cv.cv_dstValue.dv_length = cv.cv_dstDesc.ds_length;
	cv.cv_dstValue.dv_value = buf;

    IIapi_convertData(&cv);

    return cv.cv_status;
}

extern void HandleTraceMessage(IIAPI_TRACEPARM *parm);

static IIAPI_STATUS enable_trace(II_PTR env_handle)
{
    IIAPI_SETENVPRMPARM parm;
    parm.se_envHandle = env_handle;
    parm.se_paramID = IIAPI_EP_TRACE_FUNC;
    parm.se_paramValue = HandleTraceMessage;

    IIapi_setEnvParam(&parm);
}

//examples - common/aif/demo/api**.c

*/
import "C"
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unsafe"

	"database/sql/driver"
)

type OpenAPIEnv struct {
	handle C.II_PTR
}

type OpenAPIConn struct {
	env                *OpenAPIEnv
	handle             C.II_PTR
	currentTransaction *OpenAPITransaction
}

type OpenAPITransaction struct {
	conn       *OpenAPIConn
	handle     C.II_PTR
	autocommit bool
}

type ConnParams struct {
	DbName   string // vnode::dbname/server_class
	UserName string
	Password string
	Timeout  int
}

type columnDesc struct {
	ingDataType C.IIAPI_DT_ID
	name        string
	nullable    bool
	length      uint16
	precision   int16
	scale       int16

	block *colBlock
}

type rowsHeader struct {
	colNames []string
	colTyps  []columnDesc
}

// part of Columns to get
type colBlock struct {
	colIndex  uint16
	segmented bool
	count     uint16
	cols      *C.IIAPI_DATAVALUE // dv_value will point to vals[x] in rows.vals
	nulls     []*bool            // items will point to nulls[x] in rows.nulls

	buffer *bytes.Buffer
}

type colGetBlocks []*colBlock

type rows struct {
	stmt               *stmt
	transactionCreated bool

	stmtHandle C.II_PTR
	queryType  QueryType

	finish func()
	rowsHeader
	done   bool
	result driver.Result

	vals  [][]byte // byte buffers where API writes results
	nulls []bool

	/* if one or more of the columns is long we have to get columns by parts,
	   for example we have 10 columns, and 5th is long varchar, then
	   we will have three column blocks - (1-4, 5, 6-10) - it means
	   we get first 4 columns, then read segments of 5 until it's completed,
	   and then blocks 6-10
	*/
	colBlocks colGetBlocks

	lastInsertId int64
	rowsAffected int64
}

type QueryType uint

const (
	QUERY          QueryType = C.IIAPI_QT_QUERY
	SELECT_ONE     QueryType = C.IIAPI_QT_SELECT_SINGLETON
	EXEC           QueryType = C.IIAPI_QT_EXEC
	OPEN           QueryType = C.IIAPI_QT_OPEN
	EXEC_PROCEDURE QueryType = C.IIAPI_QT_EXEC_PROCEDURE
)

type stmt struct {
	conn        *OpenAPIConn
	query       string
	queryType   QueryType
	transaction *OpenAPITransaction

	args []driver.Value
}

var (
	bufferPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}

	smallBytesPool = sync.Pool{
		New: func() any {
			return make([]byte, 100)
		},
	}

	nativeEndian binary.ByteOrder
	_            driver.Result = rows{}
	verbose                    = false
)

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

func InitOpenAPI() (*OpenAPIEnv, error) {
	C.IIapi_initialize(&C.InitParm)

	if C.InitParm.in_status != 0 {
		return nil, errors.New("could not initialize Ingres OpenAPI")
	}

	handle := C.InitParm.in_envHandle
	return &OpenAPIEnv{handle: handle}, nil
}

func (env *OpenAPIEnv) EnableTrace() {
	C.enable_trace(env.handle)
}

func ReleaseOpenAPI(env *OpenAPIEnv) {
	var rel C.IIAPI_RELENVPARM
	var term C.IIAPI_TERMPARM

	rel.re_envHandle = env.handle
	C.IIapi_releaseEnv(&rel)
	C.IIapi_terminate(&term)
}

func checkError(location string, genParm *C.IIAPI_GENPARM) error {
	var status, desc string
	var err error

	if genParm.gp_status >= C.IIAPI_ST_ERROR {
		switch genParm.gp_status {
		case C.IIAPI_ST_ERROR:
			status = "IIAPI_ST_ERROR"
		case C.IIAPI_ST_FAILURE:
			status = "IIAPI_ST_FAILURE"
		case C.IIAPI_ST_NOT_INITIALIZED:
			status = "IIAPI_ST_NOT_INITIALIZED"
		case C.IIAPI_ST_INVALID_HANDLE:
			status = "IIAPI_ST_INVALID_HANDLE"
		case C.IIAPI_ST_OUT_OF_MEMORY:
			status = "IIAPI_ST_OUT_OF_MEMORY"
		default:
			status = fmt.Sprintf("%d", genParm.gp_status)
		}

		err = fmt.Errorf("%s status = %s", location, status)
	}

	if genParm.gp_errorHandle != nil {
		var getErrParm C.IIAPI_GETEINFOPARM
		/*
		 ** Provide initial error handle.
		 */
		getErrParm.ge_errorHandle = genParm.gp_errorHandle

		/*
		 ** Call IIapi_getErrorInfo() in loop until no data.
		 */
		for {
			C.IIapi_getErrorInfo(&getErrParm)
			if getErrParm.ge_status != C.IIAPI_ST_SUCCESS {
				break
			}

			/*
			 ** Print error message info.
			 */
			switch getErrParm.ge_type {
			case C.IIAPI_GE_ERROR:
				desc = "ERROR"
			case C.IIAPI_GE_WARNING:
				desc = "WARNING"
			case C.IIAPI_GE_MESSAGE:
				desc = "USER MESSAGE"
			default:
				desc = "UNKNOWN"
			}

			var msg string = "NULL"
			if getErrParm.ge_message != nil {
				msg = C.GoString(getErrParm.ge_message)
			}

			msg = fmt.Sprintf("%s: %s", desc, msg)

			state := fmt.Sprintf("%s", getErrParm.ge_SQLSTATE)
			errorCode := int(getErrParm.ge_errorCode)
			if err != nil {
				wrapped := fmt.Errorf("%w\n%s", err, msg)
				err = newIngresError(state, errorCode, wrapped)
			} else {
				err = newIngresError(state, errorCode, fmt.Errorf(msg))
			}
		}
	}

	if err != nil && verbose {
		log.Printf("%v\n", err)
	}

	return err
}

func (env *OpenAPIEnv) Connect(params ConnParams) (*OpenAPIConn, error) {
	var connParm C.IIAPI_CONNPARM

	connParm.co_genParm.gp_callback = nil
	connParm.co_genParm.gp_closure = nil
	connParm.co_type = C.IIAPI_CT_SQL
	connParm.co_target = C.CString(params.DbName)
	connParm.co_connHandle = env.handle
	connParm.co_tranHandle = nil
	connParm.co_username = nil
	connParm.co_password = nil
	if len(params.UserName) > 0 {
		connParm.co_username = C.CString(params.UserName)
	}
	if len(params.Password) > 0 {
		connParm.co_password = C.CString(params.Password)
	}

	if params.Timeout > 0 {
		connParm.co_timeout = C.int(params.Timeout)
	} else {
		connParm.co_timeout = -1
	}

	C.IIapi_connect(&connParm)
	wait(&connParm.co_genParm)
	err := checkError("IIapi_connect()", &connParm.co_genParm)

	if connParm.co_genParm.gp_status == C.IIAPI_ST_SUCCESS {
		return &OpenAPIConn{
			env:    env,
			handle: connParm.co_connHandle,
		}, nil
	}

	if connParm.co_connHandle != nil {
		var abortParm C.IIAPI_ABORTPARM

		abortParm.ab_genParm.gp_callback = nil
		abortParm.ab_genParm.gp_closure = nil
		abortParm.ab_connHandle = connParm.co_connHandle

		/*
		 ** Make sync request.
		 */
		C.IIapi_abort(&abortParm)
		wait(&abortParm.ab_genParm)

		abortErr := checkError("IIapi_abort()", &abortParm.ab_genParm)
		if verbose && abortErr != nil {
			log.Printf("could not abort connection: %v", abortErr)
		}
	}

	if err == nil {
		err = errors.New("undefined error")
	}

	return nil, err
}

func disconnect(c *OpenAPIConn) error {
	var disconnParm C.IIAPI_DISCONNPARM

	disconnParm.dc_genParm.gp_callback = nil
	disconnParm.dc_genParm.gp_closure = nil
	disconnParm.dc_connHandle = c.handle

	C.IIapi_disconnect(&disconnParm)
	wait(&disconnParm.dc_genParm)

	// Check results.
	err := checkError("IIapi_disconnect()", &disconnParm.dc_genParm)
	return err
}

func autoCommit(connHandle C.II_PTR, transHandle C.II_PTR) (C.II_PTR, error) {
	var autoParm C.IIAPI_AUTOPARM

	autoParm.ac_genParm.gp_callback = nil
	autoParm.ac_genParm.gp_closure = nil
	autoParm.ac_connHandle = connHandle
	autoParm.ac_tranHandle = transHandle

	C.IIapi_autocommit(&autoParm)
	wait(&autoParm.ac_genParm)

	/*
	 ** Check and return results.
	 **
	 ** If an error occurs enabling autocommit, the transaction
	 ** handle returned must be freed by disabling autocommit.
	 ** This is done with a extra call to this routine.
	 */
	err := checkError("IIapi_autocommit()", &autoParm.ac_genParm)
	return autoParm.ac_tranHandle, err
}

func (c *OpenAPIConn) AutoCommit() error {
	if c.currentTransaction != nil {
		return errors.New("can't enable autocommit with active transactions")
	}

	handle, err := autoCommit(c.handle, nil)
	if err != nil {
		if handle != nil {
			var nullHandle C.II_PTR = nil
			autoCommit(nullHandle, handle)
		}
		return err
	}

	c.currentTransaction = &OpenAPITransaction{conn: c, handle: handle, autocommit: true}
	return nil
}

func (c *OpenAPIConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.currentTransaction != nil {
		if c.currentTransaction.autocommit {
			err := c.DisableAutoCommit()

			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("%s", "already in transaction")
		}
	}

	s := makeStmt(c, "begin transaction", EXEC)
	rows, err := s.runQuery(nil)
	if err != nil {
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	c.currentTransaction = s.transaction
	return s.transaction, nil
}

func (c *OpenAPIConn) DisableAutoCommit() error {
	var err error

	if c.currentTransaction != nil {
		if !c.currentTransaction.autocommit {
			return errors.New("can't disable autocommit: there is ongoing transaction")
		}
		var nullHandle C.II_PTR = nil
		_, err = autoCommit(nullHandle, c.currentTransaction.handle)
	}

	c.currentTransaction = nil
	return err
}

// Wait a command to complete
func wait(genParm *C.IIAPI_GENPARM) {
	var waitParm C.IIAPI_WAITPARM

	for genParm.gp_completed == 0 {
		waitParm.wt_timeout = -1
		C.IIapi_wait(&waitParm)

		if waitParm.wt_status != C.IIAPI_ST_SUCCESS {
			genParm.gp_status = waitParm.wt_status
			break
		}
	}
}

func fillDesc(desc *C.IIAPI_DESCRIPTOR, val driver.Value) []byte {
	var resval []byte

	desc.ds_columnType = C.IIAPI_COL_QPARM
	desc.ds_columnName = nil
	desc.ds_nullable = 0
	desc.ds_precision = 0
	desc.ds_scale = 0

	switch val.(type) {
	case string:
		resval = []byte(val.(string))
		desc.ds_dataType = C.IIAPI_CHA_TYPE

	case int8, int16, int32, int64:
		var val64 uint64

		switch val.(type) {
		case int8:
			val64 = uint64(val.(int8))
		case int16:
			val64 = uint64(val.(int16))
		case int32:
			val64 = uint64(val.(int32))
		case int64:
			val64 = uint64(val.(int64))
		}
		resval = make([]byte, 8)
		nativeEndian.PutUint64(resval, val64)

		desc.ds_dataType = C.IIAPI_INT_TYPE
	case float32:
		resval = make([]byte, 4)
		nativeEndian.PutUint32(resval, math.Float32bits(val.(float32)))

		desc.ds_dataType = C.IIAPI_FLT_TYPE
	case float64:
		resval = make([]byte, 8)
		nativeEndian.PutUint64(resval, math.Float64bits(val.(float64)))

		desc.ds_dataType = C.IIAPI_FLT_TYPE
	default:
		return nil
	}

	desc.ds_length = C.uint16_t(len(resval))
	return resval
}

func (s *stmt) sendArgs(stmtHandle C.II_PTR) error {
	var err error
	var cols *C.IIAPI_DATAVALUE
	var descs *C.IIAPI_DESCRIPTOR

	var vals [][]byte

	/* cleanup everything at the end */
	defer func() {
		if cols != nil {
			C.free(unsafe.Pointer(cols))
		}

		if descs != nil {
			C.free(unsafe.Pointer(descs))
		}

		if vals != nil {
			vals = nil
		}
	}()

	if s.args == nil || len(s.args) == 0 {
		return nil
	}

	args := s.args

	var descrParm C.IIAPI_SETDESCRPARM

	cnt := C.short(len(args))
	descs = C.allocate_descs(cnt)

	descrParm.sd_descriptor = descs
	descrParm.sd_stmtHandle = stmtHandle
	descrParm.sd_descriptorCount = cnt

	cols = C.allocate_cols(descrParm.sd_descriptorCount)
	vals = make([][]byte, len(args))

	for i, arg := range args {
		val := fillDesc(C.get_desc(descs, C.ushort(i)), arg)
		if val == nil {
			return errors.New("parameter conversion error")
		}

		vals[i] = val
		C.set_dv_value(cols, C.int(i), unsafe.Pointer(&vals[i][0]))
		C.set_dv_length(cols, C.int(i), C.ushort(len(val)))
		C.set_dv_null(cols, C.int(i), 0)
	}

	C.IIapi_setDescriptor(&descrParm)
	wait(&descrParm.sd_genParm)
	err = checkError("IIapi_setDescriptor()", &descrParm.sd_genParm)
	if err != nil {
		return err
	}

	var putParm C.IIAPI_PUTPARMPARM
	putParm.pp_stmtHandle = stmtHandle
	putParm.pp_parmCount = descrParm.sd_descriptorCount
	putParm.pp_parmData = cols

	//TODO: segments support
	putParm.pp_moreSegments = 0

	C.IIapi_putParms(&putParm)
	wait(&putParm.pp_genParm)
	err = checkError("IIapi_putParms()", &putParm.pp_genParm)
	if err != nil {
		return err
	}

	return nil
}

func (s *stmt) runQuery(transHandle C.II_PTR) (*rows, error) {
	var err error
	var stmtHandle C.II_PTR
	var colBlocks colGetBlocks

	defer func() {
		if err != nil {
			closeStmt(stmtHandle)
			colBlocks.free()
		}
	}()

	var queryParm C.IIAPI_QUERYPARM
	var getDescrParm C.IIAPI_GETDESCRPARM

	queryParm.qy_genParm.gp_callback = nil
	queryParm.qy_genParm.gp_closure = nil
	queryParm.qy_connHandle = s.conn.handle
	queryParm.qy_queryType = C.uint(QUERY)
	queryParm.qy_queryText = C.CString(s.query)
	queryParm.qy_parameters = 0
	queryParm.qy_tranHandle = transHandle
	queryParm.qy_stmtHandle = nil

	if len(s.args) > 0 {
		queryParm.qy_parameters = 1
	}

	C.IIapi_query(&queryParm)
	wait(&queryParm.qy_genParm)
	err = checkError("IIapi_query()", &queryParm.qy_genParm)
	if err != nil {
		return nil, err
	}

	stmtHandle = queryParm.qy_stmtHandle
	res := &rows{
		stmt:               s,
		transactionCreated: transHandle == nil,
		stmtHandle:         stmtHandle,
	}

	s.transaction = &OpenAPITransaction{
		handle: queryParm.qy_tranHandle,
		conn:   s.conn,
	}

	if len(s.args) > 0 {
		err = s.sendArgs(res.stmtHandle)
		if err != nil {
			return nil, err
		}

		s.args = nil
	}

	// Get query result descriptors.
	if s.queryType != EXEC {
		getDescrParm.gd_genParm.gp_callback = nil
		getDescrParm.gd_genParm.gp_closure = nil
		getDescrParm.gd_stmtHandle = res.stmtHandle
		getDescrParm.gd_descriptorCount = 0
		getDescrParm.gd_descriptor = nil

		C.IIapi_getDescriptor(&getDescrParm)
		wait(&getDescrParm.gd_genParm)

		err = checkError("IIapi_getDescriptor()", &getDescrParm.gd_genParm)
		if err != nil {
			return nil, err
		}

		res.colTyps = make([]columnDesc, getDescrParm.gd_descriptorCount)
		res.colNames = make([]string, getDescrParm.gd_descriptorCount)
		res.vals = make([][]byte, len(res.colTyps))
		res.nulls = make([]bool, len(res.colTyps))

		for i := 0; i < len(res.colTyps); i++ {
			descr := C.get_descr(&getDescrParm, C.ulong(i))
			res.colTyps[i].ingDataType = descr.ds_dataType
			res.colTyps[i].nullable = (descr.ds_nullable == 1)
			res.colTyps[i].length = uint16(descr.ds_length)
			res.colTyps[i].precision = int16(descr.ds_precision)
			res.colTyps[i].scale = int16(descr.ds_scale)

			res.colNames[i] = C.GoString(descr.ds_columnName)
			res.vals[i] = make([]byte, res.colTyps[i].length)
			res.nulls[i] = false
		}

		newColBlock := func(start, end uint16, segmented bool) *colBlock {
			var i uint16

			count := end - start + 1
			if count <= 0 {
				return nil
			}

			block := &colBlock{
				count:     count,
				cols:      C.allocate_cols(C.short(count)),
				segmented: segmented,
				nulls:     make([]*bool, count),
			}

			// save link to the block for decoding

			if segmented {
				block.colIndex = start
				block.buffer = bufferPool.Get().(*bytes.Buffer)
				res.colTyps[start].block = block
			}

			for i = 0; i < count; i++ {
				j := start + i
				C.set_dv_length(block.cols, C.int(i), C.uint16_t(res.colTyps[i].length))
				C.set_dv_value(block.cols, C.int(i), unsafe.Pointer(&res.vals[j][0]))
				block.nulls[i] = &res.nulls[j]
			}
			return block
		}

		var start = uint16(0)
		var current = uint16(0)

		for current < uint16(len(res.colTyps)) {
			if res.colTyps[current].isLongType() {
				b := newColBlock(start, current-1, false)
				if b != nil {
					colBlocks = append(colBlocks, b)
				}

				colBlocks = append(colBlocks, newColBlock(current, current, true))
				start = current + 1
			}
			current += 1
		}

		b := newColBlock(start, current-1, false)
		if b != nil {
			colBlocks = append(colBlocks, b)
		}
	}

	res.colBlocks = colBlocks
	return res, nil
}

func rollbackTransaction(tranHandle C.II_PTR) error {
	var rollbackParm C.IIAPI_ROLLBACKPARM

	rollbackParm.rb_genParm.gp_callback = nil
	rollbackParm.rb_genParm.gp_closure = nil
	rollbackParm.rb_tranHandle = tranHandle
	rollbackParm.rb_savePointHandle = nil

	C.IIapi_rollback(&rollbackParm)
	wait(&rollbackParm.rb_genParm)
	return checkError("IIapi_rollback", &rollbackParm.rb_genParm)
}

func commitTransaction(tranHandle C.II_PTR) error {
	var commitParm C.IIAPI_COMMITPARM

	commitParm.cm_genParm.gp_callback = nil
	commitParm.cm_genParm.gp_closure = nil
	commitParm.cm_tranHandle = tranHandle

	C.IIapi_commit(&commitParm)
	wait(&commitParm.cm_genParm)
	return checkError("IIapi_commit", &commitParm.cm_genParm)
}

func (c *columnDesc) isLongType() bool {
	switch c.ingDataType {
	case
		C.IIAPI_LVCH_TYPE,
		C.IIAPI_LNVCH_TYPE,
		C.IIAPI_LTXT_TYPE,
		C.IIAPI_LBYTE_TYPE:
		return true
	default:
		return false
	}
}

func (c *columnDesc) splitLenVal(val []byte) (uint16, []byte) {
	switch c.ingDataType {
	case C.IIAPI_TXT_TYPE, C.IIAPI_VCH_TYPE, C.IIAPI_VBYTE_TYPE:
		cnt := nativeEndian.Uint16(val)
		val = val[2:]
		return cnt, val[:cnt]
	case C.IIAPI_NVCH_TYPE:
		cnt := nativeEndian.Uint16(val)
		val = val[2:]
		return cnt, val[:cnt*2]
	default:
		return uint16(len(val)), val
	}
}

func (c *columnDesc) getType() reflect.Type {
	switch c.ingDataType {
	case
		C.IIAPI_CHR_TYPE,
		C.IIAPI_CHA_TYPE,
		C.IIAPI_VCH_TYPE,
		C.IIAPI_LVCH_TYPE,
		C.IIAPI_NCHA_TYPE,
		C.IIAPI_NVCH_TYPE,
		C.IIAPI_LNVCH_TYPE,
		C.IIAPI_TXT_TYPE,
		C.IIAPI_LTXT_TYPE:
		return reflect.TypeOf("")
	case
		C.IIAPI_BYTE_TYPE,
		C.IIAPI_VBYTE_TYPE,
		C.IIAPI_LBYTE_TYPE:
		return reflect.TypeOf([]byte(nil))
	case C.IIAPI_INT_TYPE:
		if c.length == 2 {
			return reflect.TypeOf(int16(0))
		} else if c.length == 4 {
			return reflect.TypeOf(int32(0))
		} else if c.length == 8 {
			return reflect.TypeOf(int32(0))
		}
	case C.IIAPI_FLT_TYPE:
		if c.length == 4 {
			return reflect.TypeOf(float32(0))
		} else if c.length == 8 {
			return reflect.TypeOf(float64(0))
		}
	case
		C.IIAPI_MNY_TYPE, /* Money */
		C.IIAPI_DEC_TYPE: /* Decimal */
		return reflect.TypeOf("")
	case
		C.IIAPI_BOOL_TYPE: /* Boolean */
		return reflect.TypeOf(false)
	case
		C.IIAPI_UUID_TYPE, /* UUID */
		C.IIAPI_IPV4_TYPE, /* IPv4 */
		C.IIAPI_IPV6_TYPE: /* IPv6 */
		return reflect.TypeOf([]byte(nil))
	case
		C.IIAPI_DTE_TYPE,  /* Ingres Date */
		C.IIAPI_DATE_TYPE, /* ANSI Date */
		C.IIAPI_TIME_TYPE, /* Ingres Time */
		C.IIAPI_TMWO_TYPE, /* Time without Timezone */
		C.IIAPI_TMTZ_TYPE, /* Time with Timezone */
		C.IIAPI_TS_TYPE,   /* Ingres Timestamp */
		C.IIAPI_TSWO_TYPE, /* Timestamp without Timezone */
		C.IIAPI_TSTZ_TYPE: /* Timestamp with Timezone */
		return reflect.TypeOf(time.Time{})
	case C.IIAPI_INTYM_TYPE, /* Interval Year to Month */
		C.IIAPI_INTDS_TYPE: /* Interval Day to Second */
		return reflect.TypeOf(time.Duration(0))
	}
	return reflect.TypeOf([]byte(nil))
}

var ingresTypes = map[C.IIAPI_DT_ID]string{
	C.IIAPI_CHR_TYPE:   "c",
	C.IIAPI_CHA_TYPE:   "char",
	C.IIAPI_VCH_TYPE:   "varchar",
	C.IIAPI_LVCH_TYPE:  "long varchar",
	C.IIAPI_LCLOC_TYPE: "long char locator",
	C.IIAPI_NCHA_TYPE:  "nchar",
	C.IIAPI_NVCH_TYPE:  "nvarchar",
	C.IIAPI_LNVCH_TYPE: "long nvarchar",
	C.IIAPI_TXT_TYPE:   "text",
	C.IIAPI_LTXT_TYPE:  "long text",
	C.IIAPI_BYTE_TYPE:  "byte",
	C.IIAPI_VBYTE_TYPE: "varbyte",
	C.IIAPI_LBYTE_TYPE: "long byte",
	C.IIAPI_LBLOC_TYPE: "long byte locator",
	C.IIAPI_MNY_TYPE:   "money",
	C.IIAPI_DEC_TYPE:   "decimal",
	C.IIAPI_BOOL_TYPE:  "boolean",
	C.IIAPI_UUID_TYPE:  "UUID",
	C.IIAPI_IPV4_TYPE:  "IPV4",
	C.IIAPI_IPV6_TYPE:  "IPV6",
	C.IIAPI_DTE_TYPE:   "ingresdate",
	C.IIAPI_DATE_TYPE:  "ansidate",
	C.IIAPI_TIME_TYPE:  "time with local time zone",
	C.IIAPI_TMWO_TYPE:  "time without time zone",
	C.IIAPI_TMTZ_TYPE:  "time with time zone",
	C.IIAPI_TS_TYPE:    "timestamp with local time zone",
	C.IIAPI_TSWO_TYPE:  "timestamp without time zone",
	C.IIAPI_TSTZ_TYPE:  "timestamp with time zone",
	C.IIAPI_INTYM_TYPE: "interval year to month",
	C.IIAPI_INTDS_TYPE: "interval day to second",
}

func (c *columnDesc) getTypeName() string {
	val, ok := ingresTypes[c.ingDataType]

	if !ok {
		if c.ingDataType == C.IIAPI_INT_TYPE {
			switch c.length {
			case 1:
				return "integer1"
			case 2:
				return "integer2"
			case 4:
				return "integer4"
			case 8:
				return "integer8"
			}
		} else if c.ingDataType == C.IIAPI_FLT_TYPE {
			switch c.length {
			case 4:
				return "float4"
			case 8:
				return "float8"
			}
		}

		return "UNKNOWN"
	}

	return val
}

func (c *columnDesc) isDecimal() bool {
	return c.ingDataType == C.IIAPI_DEC_TYPE
}

var varTypes = map[C.IIAPI_DT_ID]int64{
	C.IIAPI_VCH_TYPE:   -1, // use length
	C.IIAPI_LVCH_TYPE:  2_000_000_000,
	C.IIAPI_NVCH_TYPE:  -1, // use length
	C.IIAPI_LNVCH_TYPE: 1_000_000_000,
	C.IIAPI_TXT_TYPE:   -1, // use length
	C.IIAPI_LTXT_TYPE:  -1, // use length
	C.IIAPI_VBYTE_TYPE: -1, // use length
	C.IIAPI_LBYTE_TYPE: 2_000_000_000,
}

func (c *columnDesc) Length() (int64, bool) {
	val, ok := varTypes[c.ingDataType]
	if !ok {
		return 0, false
	}

	if val == -1 {
		return int64(c.length), true
	}

	return val, true
}

func closeStmt(stmtHandle C.II_PTR) error {
	if stmtHandle != nil {
		var closeParm C.IIAPI_CLOSEPARM

		closeParm.cl_genParm.gp_callback = nil
		closeParm.cl_genParm.gp_closure = nil
		closeParm.cl_stmtHandle = stmtHandle

		C.IIapi_close(&closeParm)
		wait(&closeParm.cl_genParm)
		err := checkError("IIapi_close()", &closeParm.cl_genParm)

		if err != nil {
			return err
		}
	}
	return nil
}

func (s *stmt) Close() error {
	return nil
}

func (b colGetBlocks) free() {
	// free C allocated arrays
	for _, block := range b {
		if block.cols != nil {
			C.free(unsafe.Pointer(block.cols))
			block.cols = nil
		}

		// reuse buffers
		if block.buffer != nil {
			bufferPool.Put(block.buffer)
			block.buffer = nil
		}
	}
}

func (rs *rows) Close() error {
	if finish := rs.finish; finish != nil {
		defer finish()
	}

	err := closeStmt(rs.stmtHandle)
	if err != nil {
		return err
	}
	rs.stmtHandle = nil

	// free C allocated arrays
	rs.colBlocks.free()

	return err
}

// Gets a new row
func (rs *rows) fetchData() error {
	var err error
	var getColParm C.IIAPI_GETCOLPARM

	if rs.done {
		// do nothing
		return nil
	}

	for i := 0; i < len(rs.nulls); i++ {
		rs.nulls[i] = false
	}

	for _, block := range rs.colBlocks {
		if block.segmented {
			block.buffer.Reset()
		}

		getColParm.gc_genParm.gp_callback = nil
		getColParm.gc_genParm.gp_closure = nil
		getColParm.gc_rowCount = 1
		getColParm.gc_columnCount = C.short(block.count)
		getColParm.gc_columnData = block.cols
		getColParm.gc_stmtHandle = rs.stmtHandle

		for {
			getColParm.gc_moreSegments = 0

			C.IIapi_getColumns(&getColParm)
			wait(&getColParm.gc_genParm)
			err = checkError("IIapi_getColumns()", &getColParm.gc_genParm)

			if err != nil {
				return err
			}

			var i uint16
			for i = 0; i < block.count; i++ {
				dv := C.get_dv(block.cols, C.ushort(i))
				if dv.dv_null == 1 {
					*block.nulls[i] = true
				}
			}

			if block.segmented {
				// we can do this because for segmented there is only one
				// column in colBlock
				sz := block.cols.dv_length

				// first 2 two bytes contain the size, but we need all content
				if sz > 2 {
					block.buffer.Write(rs.vals[block.colIndex][2:sz])
				}
			}

			if getColParm.gc_moreSegments == 0 {
				break
			}
		}
	}

	if getColParm.gc_genParm.gp_status == C.IIAPI_ST_NO_DATA {
		rs.done = true
	}

	if rs.done {
		err = rs.fetchInfo()
	}

	return err
}

func (rs *rows) fetchInfo() error {
	var getQInfoParm C.IIAPI_GETQINFOPARM

	/* Get fetch result info */
	if rs.stmtHandle == nil {
		return errors.New("statement is already closed")
	}

	getQInfoParm.gq_genParm.gp_callback = nil
	getQInfoParm.gq_genParm.gp_closure = nil
	getQInfoParm.gq_stmtHandle = rs.stmtHandle

	info := &getQInfoParm
	C.IIapi_getQueryInfo(info)
	wait(&info.gq_genParm)
	err := checkError("IIapi_getQueryInfo()", &info.gq_genParm)
	if err != nil {
		rs.Close()

		return err
	}

	rs.rowsAffected = int64(info.gq_rowCountEx)
	return nil
}

func shrinkStr(res string) string {
	return strings.TrimRight(res, "\x00")
}

func shrinkStrWithBlanks(res string) string {
	return strings.TrimRight(res, "\x00 ")
}

func decode(col *columnDesc, val []byte) (driver.Value, error) {
	var res driver.Value

	_, val = col.splitLenVal(val)

	switch col.ingDataType {
	case C.IIAPI_INT_TYPE:
		switch col.length {
		case 1:
			res = int8(val[0])
		case 2:
			res = int16(nativeEndian.Uint16(val))
		case 4:
			res = int32(nativeEndian.Uint32(val))
		case 8:
			res = int64(nativeEndian.Uint64(val))
		}
	case C.IIAPI_FLT_TYPE:
		switch col.length {
		case 4:
			bits := nativeEndian.Uint32(val)
			res = math.Float32frombits(bits)
		case 8:
			bits := nativeEndian.Uint64(val)
			res = math.Float64frombits(bits)
		}
	case C.IIAPI_CHR_TYPE, C.IIAPI_CHA_TYPE:
		res = shrinkStrWithBlanks(string(val))
	case C.IIAPI_LVCH_TYPE, C.IIAPI_LTXT_TYPE:
		if col.block == nil {
			return nil, errors.New("internal: long types should have a link to column block")
		}

		// TODO: optimize here, shrink at the end for \0
		val = col.block.buffer.Bytes()
		res = shrinkStr(string(val))
	case C.IIAPI_TXT_TYPE, C.IIAPI_VCH_TYPE:
		res = shrinkStr(string(val))
	case C.IIAPI_BOOL_TYPE:
		res = (val[0] == 1)
	case C.IIAPI_VBYTE_TYPE, C.IIAPI_BYTE_TYPE:
		res = val
	case C.IIAPI_LBYTE_TYPE:
		if col.block == nil {
			return nil, errors.New("internal: long types should have a link to column block")
		}

		res = col.block.buffer.Bytes()
	case C.IIAPI_NVCH_TYPE, C.IIAPI_NCHA_TYPE:
		out := make([]uint16, len(val)/2)
		for i := range out {
			out[i] = nativeEndian.Uint16(val[i*2:])
		}
		res = string(utf16.Decode(out))
		res = shrinkStr(res.(string))
	case C.IIAPI_LNVCH_TYPE:
		val = col.block.buffer.Bytes()
		out := make([]uint16, len(val)/2)
		for i := range out {
			out[i] = nativeEndian.Uint16(val[i*2:])
		}
		res = string(utf16.Decode(out))
		res = shrinkStr(res.(string))
	case
		C.IIAPI_DEC_TYPE,   /* Decimal */
		C.IIAPI_DTE_TYPE,   /* Ingres Date */
		C.IIAPI_DATE_TYPE,  /* ANSI Date */
		C.IIAPI_TIME_TYPE,  /* Ingres Time */
		C.IIAPI_TMWO_TYPE,  /* Time without Timezone */
		C.IIAPI_TMTZ_TYPE,  /* Time with Timezone */
		C.IIAPI_TS_TYPE,    /* Ingres Timestamp */
		C.IIAPI_TSWO_TYPE,  /* Timestamp without Timezone */
		C.IIAPI_TSTZ_TYPE,  /* Timestamp with Timezone */
		C.IIAPI_INTYM_TYPE, /* Interval Year to Month */
		C.IIAPI_INTDS_TYPE: /* Interval Day to Second */
		var err error

		res, err = convertToStr(col, val)
		if err != nil {
			return nil, err
		}
		res = shrinkStrWithBlanks(res.(string))
	default:
		return nil, errors.New("type is not supported")
	}

	return res, nil
}

func convertToStr(cd *columnDesc, val []byte) (string, error) {
	var destBuf = smallBytesPool.Get().([]byte)

	status := C.convert_to_string(cd.ingDataType, C.II_PTR(&val[0]), C.ushort(len(val)),
		C.II_PTR(&destBuf[0]), C.ushort(len(destBuf)))

	if status != C.IIAPI_ST_SUCCESS {
		return "", errors.New("conversion error")
	}

	res := string(destBuf)
	smallBytesPool.Put(destBuf)

	return res, nil
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	err = rs.fetchData()
	if err != nil {
		return err
	}

	if rs.done {
		return io.EOF
	}

	for i, val := range rs.vals {
		if rs.nulls[i] {
			dest[i] = nil
			continue
		}

		dest[i], err = decode(&rs.colTyps[i], val)
		if err != nil {
			return err
		}
	}

	return nil
}

// Compile time validation that our types implement the expected interfaces
var (
	_   driver.Driver = Driver{}
	env *OpenAPIEnv
)

// Driver is the Ingres database driver.
type Driver struct{}

type IngresError struct {
	State     string
	ErrorCode int
	Err       error
}

func newIngresError(state string, code int, err error) *IngresError {
	return &IngresError{
		State:     state,
		ErrorCode: code,
		Err:       err,
	}
}

func (e *IngresError) Error() string {
	return fmt.Sprintf("%v", e.Err)
}

func (e *IngresError) Unwrap() error {
	return e.Err
}

func init() {
	var err error
	env, err = InitOpenAPI()
	if err != nil {
		log.Fatalf("could not initialize OpenAPI: %v", err)
	}

	env.EnableTrace()

	d := &Driver{}
	sql.Register("ingres", d)
}

func (d Driver) Open(name string) (driver.Conn, error) {
	var params ConnParams

	if strings.Contains(name, "?") {
		parts := strings.Split(name, "?")
		if len(parts) != 2 {
			return nil, errors.New("DSN is invalid")
		}

		values, err := url.ParseQuery(parts[1])

		if err != nil {
			return nil, errors.New("parameters parse error")
		}

		if values.Has("username") && !values.Has("password") {
			return nil, errors.New("password has not been specified")
		}

		params.UserName = values.Get("username")
		params.Password = values.Get("password")

		name = parts[0]
	}

	params.DbName = name
	conn, err := env.Connect(params)
	if err != nil {
		return nil, err
	}
	err = conn.AutoCommit()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func makeStmt(c *OpenAPIConn, query string, queryType QueryType) *stmt {
	return &stmt{
		args:      nil,
		conn:      c,
		query:     strings.TrimRight(query, "; "),
		queryType: queryType,
	}
}

func (c *OpenAPIConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	s := makeStmt(c, query, QUERY)
	return s.Query(args)
}

func (c *OpenAPIConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	s := makeStmt(c, query, EXEC)
	return s.Exec(args)
}

func (c *OpenAPIConn) Prepare(query string) (driver.Stmt, error) {
	return makeStmt(c, query, QUERY), nil
}

func (c *OpenAPIConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *OpenAPIConn) Close() error {
	return disconnect(c)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	var rows *rows
	var err error

	s.queryType = EXEC
	if len(args) > 0 {
		s.args = args
	}

	if s.conn.currentTransaction == nil {
		return nil, errors.New("transaction required")
	}
	rows, err = s.runQuery(s.conn.currentTransaction.handle)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	err = rows.fetchInfo()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.queryType = QUERY
	s.args = args

	if s.conn.currentTransaction == nil {
		return nil, errors.New("transaction required")
	}

	return s.runQuery(s.conn.currentTransaction.handle)
}

func (s *stmt) NumInput() int {
	return -1
}

func (t *OpenAPITransaction) Commit() error {
	err := commitTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
		t.conn.AutoCommit()
	}
	return err
}

func (t *OpenAPITransaction) Rollback() error {
	err := rollbackTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
		t.conn.AutoCommit()
	}
	return err
}

//export HandleTraceMessage
func HandleTraceMessage(parm *C.IIAPI_TRACEPARM) {
	fmt.Print(C.GoString(parm.tr_message))
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (rs *rows) ColumnTypeScanType(index int) reflect.Type {
	return rs.colTyps[index].getType()
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (rs *rows) ColumnTypeDatabaseTypeName(index int) string {
	return rs.colTyps[index].getTypeName()
}

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (rs *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return rs.colTyps[index].Length()
}

func (rs *rows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	return rs.colTyps[index].nullable, true
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	desc := rs.colTyps[index]
	if desc.isDecimal() {
		return int64(rs.colTyps[index].precision), int64(rs.colTyps[index].scale), true
	}
	return 0, 0, false
}

func (rs *rows) Columns() []string {
	return rs.colNames
}

func (rs rows) LastInsertId() (int64, error) {
	return rs.lastInsertId, nil
}

func (rs rows) RowsAffected() (int64, error) {
	return rs.rowsAffected, nil
}
