package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"schenker/pg-proxy/pgbroker/backend"
	"schenker/pg-proxy/pgbroker/message"
	"schenker/pg-proxy/pgbroker/proxy"
	"schenker/pg-proxy/sqlutils"
)

type ProxyInstance struct {
	Host                      string
	Port                      int
	Remote                    string
	CertificateFile           string
	KeyFile                   string
	Verbose                   int
	KeepOriginal              bool
	StartupParametersOverride map[string]string
	sqlutils.Translator

	queryStore   *queryStore
	polyfillLock *sync.RWMutex
}

var _ backend.PGStartupMessageRewriter = (*ProxyInstance)(nil)
var _ backend.PGResolver = (*ProxyInstance)(nil)

type QueryContext struct {
	Time        time.Time `json:"time"`
	Results     int64     `json:"results"`
	Duration    int64     `json:"duration"`
	ClientInfo  string    `json:"client"`
	OriginalSQL string    `json:"original"`
	FinalSQL    string    `json:"final"` // empty when not transformed
	Error       string    `json:"error"` // translation error, if any
	Query       *sqlutils.Query

	ongoingCopyQuery bool
	prepared         bool
}

func (instance *ProxyInstance) GetPGConn(ctx context.Context, clientAddr net.Addr, parameters map[string]string) (net.Conn, error) {
	remote, ok := parameters["proxy.remote"]
	if ok && remote != "" {
		return net.Dial("tcp", remote)
	}

	database, ok := parameters["database"]
	if ok && strings.Contains(database, "@") {
		arobase := strings.Index(database, "@")
		parameters["database"] = database[:arobase]
		remote = database[(arobase + 1):]
		parameters["proxy.remote"] = remote
		return net.Dial("tcp", remote)
	}
	if len(instance.Remote) == 0 {
		return nil, fmt.Errorf("remote host not found ! Try 'database@host:port' as the database name (found %s)", database)
	}
	return net.Dial("tcp", instance.Remote)
}

func (instance *ProxyInstance) RewriteParameters(original map[string]string) map[string]string {
	if instance.StartupParametersOverride != nil {
		maps.Copy(original, instance.StartupParametersOverride)
	}
	return original
}

func (instance *ProxyInstance) getQueryContext(ctx *proxy.Ctx) *QueryContext {
	if ctx != nil && ctx.QueryContext != nil {
		ctxt, ok := ctx.QueryContext.(*QueryContext)
		if ok {
			return ctxt
		}
	}
	return nil
}

func (instance *ProxyInstance) handleConnError(err error, ctx *proxy.Ctx, conn net.Conn) {
	queryCtxt := instance.getQueryContext(ctx)
	if err != io.EOF && instance.Verbose&1 == 1 && queryCtxt != nil {
		log.Printf("WARN  [%s] Connection error : %v", queryCtxt.ClientInfo, err)
	}
}

func (instance *ProxyInstance) handleParse(ctx *proxy.Ctx, msg *message.Parse) (parse *message.Parse, e error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	if queryCtxt.OriginalSQL != "" {
		// query not previously logged
		instance.traceQuery(ctx)
	}
	queryCtxt.Time = time.Now()
	queryCtxt.OriginalSQL = msg.QueryString
	queryCtxt.FinalSQL = ""
	queryCtxt.prepared = true
	queryCtxt.ongoingCopyQuery = false
	var err error
	queryCtxt.Query, err = instance.Translate(msg.QueryString)
	if err != nil {
		queryCtxt.Error = err.Error()
	}
	if queryCtxt.Query == nil || !queryCtxt.Query.Transformed {
		return msg, nil
	} else if queryCtxt.Query.LocalCopy == "$1" {
		msg.ParameterIDs = []uint32{25}
	}
	queryCtxt.ongoingCopyQuery = queryCtxt.Query.LocalCopy != ""
	queryCtxt.FinalSQL = queryCtxt.Query.Sql()
	msg.QueryString = queryCtxt.FinalSQL
	if instance.KeepOriginal {
		msg.QueryString += " --translated from:\n-- " + strings.ReplaceAll(strings.ReplaceAll(queryCtxt.OriginalSQL, "\n", "\n-- "), "\r", "")
	}
	return msg, nil
}

func (instance *ProxyInstance) handleQuery(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	parseMsg, _ := instance.handleParse(ctx, &message.Parse{
		QueryString: msg.QueryString,
	})
	queryCtxt.prepared = false
	if parseMsg != nil {
		msg.QueryString = parseMsg.QueryString
	}
	return msg, nil
}

func (instance *ProxyInstance) sendDataToServer(ctx *proxy.Ctx, reader io.Reader, finalExpectedType byte) ([]byte, error) {
	if _, err := io.Copy(ctx.ServerConn, reader); err != nil {
		return nil, fmt.Errorf("data write failed: %w", err)
	}

	var result error
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(ctx.ServerConn, header); err != nil {
			return nil, fmt.Errorf("data read failed: %w", err)
		}

		msgType := header[0]
		bodyLen := int(binary.BigEndian.Uint32(header[1:5])) - 4

		var body []byte
		if bodyLen > 0 {
			body = make([]byte, bodyLen)
			if _, err := io.ReadFull(ctx.ServerConn, body); err != nil {
				return nil, fmt.Errorf("data read failed: %w", err)
			}
		}

		if msgType == 'E' {
			result = errors.New("data error")
			errResp := message.ReadErrorResponse(body)
			for _, f := range errResp.Fields {
				if f.Type == 'M' {
					result = errors.New(f.Value)
					break
				}
			}
		}

		if msgType == finalExpectedType {
			return body, result
		}
	}
}

func (instance *ProxyInstance) traceQuery(ctx *proxy.Ctx) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt == nil {
		return
	}
	if queryCtxt.OriginalSQL != "" && !queryCtxt.ongoingCopyQuery {
		queryCtxt.Duration = time.Since(queryCtxt.Time).Microseconds()
		if instance.queryStore != nil {
			instance.queryStore.add(queryRecord{
				Time:        queryCtxt.Time,
				Duration:    queryCtxt.Duration,
				Results:     queryCtxt.Results,
				ClientInfo:  queryCtxt.ClientInfo,
				OriginalSQL: queryCtxt.OriginalSQL,
				FinalSQL:    queryCtxt.FinalSQL,
				Error:       queryCtxt.Error,
			})
		}
		query := queryCtxt.OriginalSQL
		if queryCtxt.FinalSQL != "" {
			query = fmt.Sprintf("%s {original: %s}", queryCtxt.FinalSQL, queryCtxt.OriginalSQL)
		}
		if queryCtxt.Error != "" {
			log.Printf("ERROR [%s] %s, when executing: %s\n", queryCtxt.ClientInfo, queryCtxt.Error, query)
		} else if instance.Verbose&4 == 4 || (instance.Verbose&2 == 2 && queryCtxt.FinalSQL != "") {
			log.Printf("INFO  [%s] %s {%d results in %d µs}\n", queryCtxt.ClientInfo, query, queryCtxt.Results, queryCtxt.Duration)
		}
		queryCtxt.Time = time.Now()
		queryCtxt.Duration = 0
		queryCtxt.Results = 0
		queryCtxt.OriginalSQL = ""
		queryCtxt.FinalSQL = ""
	}
	queryCtxt.Error = ""
}

func (instance *ProxyInstance) managePolyfills(ctx *proxy.Ctx) error {
	polyfills := instance.Polyfills()
	if polyfills == nil {
		return nil
	}
	if polyfills.SessionCreate != "" {
		_, err := instance.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SessionCreate}).Reader(), 'Z')
		if err != nil {
			return err
		}
	}
	if polyfills.Polyfilled {
		return nil
	}
	instance.polyfillLock.Lock()
	defer instance.polyfillLock.Unlock()
	if polyfills.SystemCheck == "" || polyfills.SystemCreate == "" {
		polyfills.Polyfilled = true
		return nil
	}
	_, err := instance.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SystemCheck}).Reader(), 'Z')
	if err != nil { // seems polyfill is not installed
		_, err = instance.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SystemCreate}).Reader(), 'Z')
		if err != nil {
			return err
		}
	}
	polyfills.Polyfilled = true
	return nil
}

func (instance *ProxyInstance) handleCommandComplete(ctx *proxy.Ctx, msg *message.CommandComplete) (*message.CommandComplete, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if msg != nil && queryCtxt != nil {
		parts := strings.Split(msg.CommandTag, " ")
		if len(parts) > 0 {
			res, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
			if err == nil {
				queryCtxt.Results = res
			}
		}
	}
	instance.traceQuery(ctx)
	return msg, nil
}

func (instance *ProxyInstance) handleTerminate(ctx *proxy.Ctx, msg *message.Terminate) (*message.Terminate, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if instance.Verbose&1 == 1 && queryCtxt != nil {
		log.Printf("INFO  [%s] Client sent termination\n", queryCtxt.ClientInfo)
	}
	instance.traceQuery(ctx)
	return msg, nil
}

func (instance *ProxyInstance) handleAuthenticationOk(ctx *proxy.Ctx, msg *message.AuthenticationOk) (*message.AuthenticationOk, error) {
	if instance.Verbose&1 == 1 {
		log.Printf("INFO  [%s] Server authentification OK (%d)\n", fmt.Sprintf("%-6d %-40s", 0, fmt.Sprintf("%v (%v)", ctx.ConnInfo.StartupParameters["user"],
			ctx.ConnInfo.ClientAddress)), msg.ID)
	}
	return msg, nil
}

func (instance *ProxyInstance) handleBackendKeyData(ctx *proxy.Ctx, msg *message.BackendKeyData) (*message.BackendKeyData, error) {
	ctx.QueryContext = &QueryContext{
		ClientInfo: fmt.Sprintf("%-6d %-40s", msg.ProcessID, fmt.Sprintf("%v (%v)", ctx.ConnInfo.StartupParameters["user"],
			ctx.ConnInfo.ClientAddress)),
	}
	err := instance.managePolyfills(ctx)
	return msg, err
}

func (instance *ProxyInstance) handleParameterDescription(ctx *proxy.Ctx, msg *message.ParameterDescription) (*message.ParameterDescription, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Query != nil && queryCtxt.Query.LocalCopy == "$1" && queryCtxt.prepared {
		msg.ParameterIDs = []uint32{25} // OID TEXT
	}
	return msg, nil
}

func (instance *ProxyInstance) handleBind(ctx *proxy.Ctx, msg *message.Bind) (*message.Bind, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Query != nil && queryCtxt.Query.LocalCopy == "$1" &&
		queryCtxt.prepared && len(msg.ParameterValues) > 0 {
		queryCtxt.Query.LocalCopy = string(msg.ParameterValues[0].DataBytes())
	}
	return msg, nil
}

func (instance *ProxyInstance) handleRowDescription(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
	for i := range msg.Fields {
		msg.Fields[i].Name = instance.RenameRowField(i, msg.Fields[i].Name)
	}
	return msg, nil
}

func (instance *ProxyInstance) handleCopyInResponse(ctx *proxy.Ctx, msg *message.CopyInResponse) (*message.CopyInResponse, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	queryCtxt.ongoingCopyQuery = false
	if queryCtxt.Query != nil && queryCtxt.Query.LocalCopy != "" {
		if instance.Verbose&2 == 2 || instance.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy from %s\n", queryCtxt.ClientInfo, queryCtxt.Query.LocalCopy)
		}
		f, err := os.Open(queryCtxt.Query.LocalCopy)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		buf := make([]byte, 65536)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				if instance.Verbose&2 == 2 || instance.Verbose&4 == 4 {
					log.Printf("INFO  [%s] Read %d bytes from %s\n", queryCtxt.ClientInfo, n, queryCtxt.Query.LocalCopy)
				}
				chunk := buf[:n]
				if _, err := io.Copy(ctx.ServerConn, message.ReadCopyData(chunk).Reader()); err != nil {
					return nil, fmt.Errorf("Copy Data send failed: %w", err)
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return msg, err
			}
		}
		if _, err := io.Copy(ctx.ServerConn, message.ReadCopyDone([]byte{}).Reader()); err != nil {
			return nil, fmt.Errorf("Copy Done send failed: %w", err)
		}
		if queryCtxt.prepared {
			if _, err := io.Copy(ctx.ServerConn, message.ReadSync([]byte{}).Reader()); err != nil {
				return nil, fmt.Errorf("Sync send failed: %w", err)
			}
		}
		if instance.Verbose&2 == 2 || instance.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy from %s done\n", queryCtxt.ClientInfo, queryCtxt.Query.LocalCopy)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (instance *ProxyInstance) handleCopyOutResponse(ctx *proxy.Ctx, msg *message.CopyOutResponse) (*message.CopyOutResponse, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	queryCtxt.ongoingCopyQuery = false
	if queryCtxt.Query != nil && queryCtxt.Query.LocalCopy != "" {
		if instance.Verbose&2 == 2 || instance.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy to %s\n", queryCtxt.ClientInfo, queryCtxt.Query.LocalCopy)
		}
		f, err := os.Create(queryCtxt.Query.LocalCopy)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		msg.BypassReturn = true
	}
	return msg, nil
}

func (instance *ProxyInstance) handleCopyData(ctx *proxy.Ctx, msg *message.CopyData) (*message.CopyData, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Query != nil && queryCtxt.Query.LocalCopy != "" {
		f, err := os.OpenFile(queryCtxt.Query.LocalCopy, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		_, err = io.Copy(f, bytes.NewReader(msg.Data))
		if err != nil {
			return msg, err
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (instance *ProxyInstance) handleCopyDone(ctx *proxy.Ctx, msg *message.CopyDone) (*message.CopyDone, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Query != nil && queryCtxt.Query.LocalCopy != "" {
		if instance.Verbose&2 == 2 || instance.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy to %s done\n", queryCtxt.ClientInfo, queryCtxt.Query.LocalCopy)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (instance *ProxyInstance) handleErrorResponse(ctx *proxy.Ctx, msg *message.ErrorResponse) (*message.ErrorResponse, error) {
	queryCtxt := instance.getQueryContext(ctx)
	if queryCtxt != nil {
		queryCtxt.ongoingCopyQuery = false
		var errorMessage string
		var errorCode string
		for i, err := range msg.Fields {
			switch err.Type {
			case 77:
				errorMessage = err.Value
				if queryCtxt.OriginalSQL != "" && queryCtxt.Error == "" { // erreur postgres sans traduction en erreur
					queryCtxt.Error = err.Value
				} else if queryCtxt.Error != "" { // erreur interne du proxy
					errorMessage = fmt.Sprintf("pg-proxy error: %v (postgres error: %s)", queryCtxt.Error, err.Value)
					// on surcharge le retour d'erreur pour le client
					newFields := append(msg.Fields[0:i], message.ErrorField{
						Type:  err.Type,
						Value: errorMessage,
					})
					msg.Fields = append(newFields, msg.Fields[i+1:]...)
				}

			case 67:
				errorCode = err.Value
			}
		}
		if errorMessage != "" {
			queryCtxt.Error = fmt.Sprintf("%s (error code: %s)", errorMessage, errorCode)
		}
		instance.traceQuery(ctx)
	}
	return msg, nil
}

func (instance *ProxyInstance) NewSelfSignedCert() (tls.Certificate, error) {
	var outCert tls.Certificate
	var host = instance.Host
	if len(host) == 0 {
		host = "localhost"
	}
	now := time.Now()
	rawValues := []asn1.RawValue{}
	rawValues = append(rawValues, asn1.RawValue{
		Bytes: []byte(host),
		Class: asn1.ClassContextSpecific,
		Tag:   2, // DNS name
	})
	asn, err := asn1.Marshal(rawValues)
	if err != nil {
		return outCert, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.Unix()),
		Subject: pkix.Name{
			CommonName: host,
		},
		ExtraExtensions: []pkix.Extension{
			{
				Id:    asn1.ObjectIdentifier{2, 5, 29, 17},
				Value: asn,
			},
		},
		NotBefore:    now,
		NotAfter:     now.AddDate(10, 0, 0), // Valid for 10 years
		SubjectKeyId: []byte{113, 117, 105, 99, 107, 115, 101, 114, 118, 101},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageKeyEncipherment |
			x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return outCert, err
	}

	cert, err := x509.CreateCertificate(rand.Reader, template, template,
		priv.Public(), priv)
	if err != nil {
		return outCert, err
	}

	outCert.Certificate = append(outCert.Certificate, cert)
	outCert.PrivateKey = priv
	return outCert, err
}

func (instance *ProxyInstance) NewServer() (*proxy.Server, error) {

	var tlsConfig *tls.Config
	if len(instance.CertificateFile) > 0 {
		tlsConfig = &tls.Config{}
		if len(instance.KeyFile) > 0 {
			cert, err := tls.LoadX509KeyPair(instance.CertificateFile, instance.KeyFile)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		} else {
			cert, err := instance.NewSelfSignedCert()
			if err != nil {
				return nil, err
			}
			log.Printf("KeyFile not defined : server now use a self signed certificate")
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	clientMessageHandlers := proxy.NewClientMessageHandlers()
	serverMessageHandlers := proxy.NewServerMessageHandlers()

	clientMessageHandlers.AddHandleQuery(instance.handleQuery)
	clientMessageHandlers.AddHandleParse(instance.handleParse)
	clientMessageHandlers.AddHandleTerminate(instance.handleTerminate)
	clientMessageHandlers.AddHandleBind(instance.handleBind)
	serverMessageHandlers.AddHandleBackendKeyData(instance.handleBackendKeyData)
	serverMessageHandlers.AddHandleParameterDescription(instance.handleParameterDescription)
	serverMessageHandlers.AddHandleRowDescription(instance.handleRowDescription)
	serverMessageHandlers.AddHandleCommandComplete(instance.handleCommandComplete)
	serverMessageHandlers.AddHandleAuthenticationOk(instance.handleAuthenticationOk)
	serverMessageHandlers.AddHandleErrorResponse(instance.handleErrorResponse)
	serverMessageHandlers.AddHandleCopyInResponse(instance.handleCopyInResponse)
	serverMessageHandlers.AddHandleCopyOutResponse(instance.handleCopyOutResponse)
	serverMessageHandlers.AddHandleCopyData(instance.handleCopyData)
	serverMessageHandlers.AddHandleCopyDone(instance.handleCopyDone)

	if instance.polyfillLock == nil {
		instance.polyfillLock = &sync.RWMutex{}
	}
	if instance.Translator == nil {
		instance.Translator = sqlutils.IsoTranslator()
	}
	return &proxy.Server{
		TLSConfig:                tlsConfig,
		PGResolver:               instance,
		PGStartupMessageRewriter: instance,
		ConnInfoStore:            backend.NewInMemoryConnInfoStore(),
		ClientMessageHandlers:    clientMessageHandlers,
		ServerMessageHandlers:    serverMessageHandlers,
		OnHandleConnError:        instance.handleConnError,
		ProtocolDebug:            instance.Verbose&8 == 8,
	}, nil
}
