package main

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
	"strings"
	"sync"
	"time"

	"schenker/pg-proxy/pgbroker/backend"
	"schenker/pg-proxy/pgbroker/message"
	"schenker/pg-proxy/pgbroker/proxy"
)

type ProxyConfig struct {
	Host                      string
	Port                      int
	Remote                    string
	CertificateFile           string
	KeyFile                   string
	Verbose                   int
	KeepOriginal              bool
	StartupParametersOverride map[string]string
	QueryStore                *QueryStore
	SqlTranslator

	systemPolyfilled bool
	polyfillLock     *sync.RWMutex
}

var _ backend.PGStartupMessageRewriter = (*ProxyConfig)(nil)
var _ backend.PGResolver = (*ProxyConfig)(nil)

type QueryContext struct {
	Time        time.Time `json:"time"`
	ClientInfo  string    `json:"client"`
	OriginalSQL string    `json:"original"`
	FinalSQL    string    `json:"final"` // empty when not transformed
	Error       string    `json:"error"` // translation error, if any
	Translated  *SqlQuery

	ongoingCopyQuery bool
	prepared         bool
}

func (config *ProxyConfig) GetPGConn(ctx context.Context, clientAddr net.Addr, parameters map[string]string) (net.Conn, error) {
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
	if len(config.Remote) == 0 {
		return nil, fmt.Errorf("remote host not found ! Try 'database@host:port' as the database name (found %s)", database)
	}
	return net.Dial("tcp", config.Remote)
}

func (config *ProxyConfig) RewriteParameters(original map[string]string) map[string]string {
	if config.StartupParametersOverride != nil {
		maps.Copy(original, config.StartupParametersOverride)
	}
	return original
}

func (config *ProxyConfig) getQueryContext(ctx *proxy.Ctx) *QueryContext {
	if ctx != nil && ctx.QueryContext != nil {
		ctxt, ok := ctx.QueryContext.(*QueryContext)
		if ok {
			return ctxt
		}
	}
	return nil
}

func (config *ProxyConfig) handleConnError(err error, ctx *proxy.Ctx, conn net.Conn) {
	queryCtxt := config.getQueryContext(ctx)
	if err != io.EOF && config.Verbose&1 == 1 && queryCtxt != nil {
		log.Printf("WARN  [%s] Connection error : %v", queryCtxt.ClientInfo, err)
	}
}

func (config *ProxyConfig) handleParse(ctx *proxy.Ctx, msg *message.Parse) (parse *message.Parse, e error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	queryCtxt.Time = time.Now()
	queryCtxt.OriginalSQL = msg.QueryString
	queryCtxt.FinalSQL = ""
	queryCtxt.prepared = true
	queryCtxt.ongoingCopyQuery = false
	var err error
	queryCtxt.Translated, err = config.Translate(msg.QueryString, config.systemPolyfilled)
	if err != nil {
		queryCtxt.Error = err.Error()
	}
	if queryCtxt.Translated == nil || !queryCtxt.Translated.Transformed {
		if config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] %s\n", queryCtxt.ClientInfo, msg.QueryString)
		}
		return msg, nil
	} else if queryCtxt.Translated.LocalCopy == "$1" {
		msg.ParameterIDs = []uint32{25}
	}
	queryCtxt.ongoingCopyQuery = queryCtxt.Translated.LocalCopy != ""
	queryCtxt.FinalSQL = queryCtxt.Translated.Sql()
	msg.QueryString = queryCtxt.FinalSQL
	if config.KeepOriginal {
		msg.QueryString += " --translated from:\n-- " + strings.ReplaceAll(strings.ReplaceAll(queryCtxt.OriginalSQL, "\n", "\n-- "), "\r", "")
	}
	if config.Verbose&2 == 2 {
		log.Printf("INFO  [%s] %s in %d µs\n", queryCtxt.ClientInfo, msg.QueryString, time.Since(queryCtxt.Time).Microseconds())
	}
	return msg, nil
}

func (config *ProxyConfig) handleQuery(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	parseMsg, _ := config.handleParse(ctx, &message.Parse{
		QueryString: msg.QueryString,
	})
	queryCtxt.prepared = false
	if parseMsg != nil {
		msg.QueryString = parseMsg.QueryString
	}
	return msg, nil
}

func (config *ProxyConfig) sendDataToServer(ctx *proxy.Ctx, reader io.Reader, finalExpectedType byte) ([]byte, error) {
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

func (config *ProxyConfig) cleanupStore(ctx *proxy.Ctx) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt == nil {
		return
	}
	if queryCtxt.OriginalSQL != "" && !queryCtxt.ongoingCopyQuery {
		if config.QueryStore != nil {
			config.QueryStore.Add(QueryRecord{
				Time:        queryCtxt.Time,
				Duration:    time.Since(queryCtxt.Time).Milliseconds(),
				ClientInfo:  queryCtxt.ClientInfo,
				OriginalSQL: queryCtxt.OriginalSQL,
				FinalSQL:    queryCtxt.FinalSQL,
				Error:       queryCtxt.Error,
			})
		}
		queryCtxt.OriginalSQL = ""
		queryCtxt.FinalSQL = ""
	}
	queryCtxt.Error = ""
}

func (config *ProxyConfig) managePolyfills(ctx *proxy.Ctx) error {
	polyfills := config.Polyfills()
	if polyfills == nil {
		return nil
	}
	if polyfills.SessionCreate != "" {
		_, err := config.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SessionCreate}).Reader(), 'Z')
		if err != nil {
			return err
		}
	}
	if config.systemPolyfilled {
		return nil
	}
	config.polyfillLock.Lock()
	defer config.polyfillLock.Unlock()
	if polyfills.SystemCheck == "" || polyfills.SystemCreate == "" {
		config.systemPolyfilled = true
		return nil
	}
	_, err := config.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SystemCheck}).Reader(), 'Z')
	if err != nil { // seems polyfill is not installed
		_, err = config.sendDataToServer(ctx, (&message.Query{QueryString: polyfills.SystemCreate}).Reader(), 'Z')
		if err != nil {
			return err
		}
	}
	config.systemPolyfilled = true
	return nil
}

func (config *ProxyConfig) handleReadyForQuery(ctx *proxy.Ctx, msg *message.ReadyForQuery) (*message.ReadyForQuery, error) {
	config.cleanupStore(ctx)
	return msg, nil
}

func (config *ProxyConfig) handleTerminate(ctx *proxy.Ctx, msg *message.Terminate) (*message.Terminate, error) {
	queryCtxt := config.getQueryContext(ctx)
	if config.Verbose&1 == 1 && queryCtxt != nil {
		log.Printf("INFO  [%s] Client sent termination\n", queryCtxt.ClientInfo)
	}
	config.cleanupStore(ctx)
	return msg, nil
}

func (config *ProxyConfig) handleAuthenticationOk(ctx *proxy.Ctx, msg *message.AuthenticationOk) (*message.AuthenticationOk, error) {
	if config.Verbose&1 == 1 {
		log.Printf("INFO  [%s] Server authentification OK (%d)\n", fmt.Sprintf("%-6d %-40s", 0, fmt.Sprintf("%v (%v)", ctx.ConnInfo.StartupParameters["user"],
			ctx.ConnInfo.ClientAddress)), msg.ID)
	}
	return msg, nil
}

func (config *ProxyConfig) handleBackendKeyData(ctx *proxy.Ctx, msg *message.BackendKeyData) (*message.BackendKeyData, error) {
	ctx.QueryContext = &QueryContext{
		ClientInfo: fmt.Sprintf("%-6d %-40s", msg.ProcessID, fmt.Sprintf("%v (%v)", ctx.ConnInfo.StartupParameters["user"],
			ctx.ConnInfo.ClientAddress)),
	}
	err := config.managePolyfills(ctx)
	return msg, err
}

func (config *ProxyConfig) handleParameterDescription(ctx *proxy.Ctx, msg *message.ParameterDescription) (*message.ParameterDescription, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy == "$1" && queryCtxt.prepared {
		msg.ParameterIDs = []uint32{25} // OID TEXT
	}
	return msg, nil
}

func (config *ProxyConfig) handleBind(ctx *proxy.Ctx, msg *message.Bind) (*message.Bind, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy == "$1" &&
		queryCtxt.prepared && len(msg.ParameterValues) > 0 {
		queryCtxt.Translated.LocalCopy = string(msg.ParameterValues[0].DataBytes())
	}
	return msg, nil
}

func (config *ProxyConfig) handleRowDescription(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
	for i := range msg.Fields {
		msg.Fields[i].Name = config.RenameRowField(i, msg.Fields[i].Name)
	}
	return msg, nil
}

func (config *ProxyConfig) handleCopyInResponse(ctx *proxy.Ctx, msg *message.CopyInResponse) (*message.CopyInResponse, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	queryCtxt.ongoingCopyQuery = false
	if queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy != "" {
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy from %s\n", queryCtxt.ClientInfo, queryCtxt.Translated.LocalCopy)
		}
		f, err := os.Open(queryCtxt.Translated.LocalCopy)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		buf := make([]byte, 65536)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
					log.Printf("INFO  [%s] Read %d bytes from %s\n", queryCtxt.ClientInfo, n, queryCtxt.Translated.LocalCopy)
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
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy from %s done\n", queryCtxt.ClientInfo, queryCtxt.Translated.LocalCopy)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleCopyOutResponse(ctx *proxy.Ctx, msg *message.CopyOutResponse) (*message.CopyOutResponse, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt == nil {
		return msg, nil
	}
	queryCtxt.ongoingCopyQuery = false
	if queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy != "" {
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy to %s\n", queryCtxt.ClientInfo, queryCtxt.Translated.LocalCopy)
		}
		f, err := os.Create(queryCtxt.Translated.LocalCopy)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleCopyData(ctx *proxy.Ctx, msg *message.CopyData) (*message.CopyData, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy != "" {
		f, err := os.OpenFile(queryCtxt.Translated.LocalCopy, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

func (config *ProxyConfig) handleCopyDone(ctx *proxy.Ctx, msg *message.CopyDone) (*message.CopyDone, error) {
	queryCtxt := config.getQueryContext(ctx)
	if queryCtxt != nil && queryCtxt.Translated != nil && queryCtxt.Translated.LocalCopy != "" {
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy to %s done\n", queryCtxt.ClientInfo, queryCtxt.Translated.LocalCopy)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleErrorResponse(ctx *proxy.Ctx, msg *message.ErrorResponse) (*message.ErrorResponse, error) {
	queryCtxt := config.getQueryContext(ctx)
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
					errorMessage = fmt.Sprintf("%v (from query: %s)", err.Value, queryCtxt.OriginalSQL)
				} else if queryCtxt.Error != "" { // erreur interne du proxy
					if queryCtxt.OriginalSQL == "" {
						queryCtxt.OriginalSQL = "<unknown>"
					}
					errorMessage = fmt.Sprintf("pg-proxy error: %v (from query: %s) (postgres error: %s)", queryCtxt.Error, queryCtxt.OriginalSQL, err.Value)
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
		config.cleanupStore(ctx)
		if (config.Verbose&2 == 2 || config.Verbose&4 == 4) && len(errorMessage) > 0 {
			log.Printf("ERROR [%s] %s (error code: %s)\n", queryCtxt.ClientInfo, errorMessage, errorCode)
		}
	}
	return msg, nil
}

func (config *ProxyConfig) NewSelfSignedCert() (tls.Certificate, error) {
	var outCert tls.Certificate
	var host = config.Host
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

func (config *ProxyConfig) NewServer() (*proxy.Server, error) {

	var tlsConfig *tls.Config
	if len(config.CertificateFile) > 0 {
		tlsConfig = &tls.Config{}
		if len(config.KeyFile) > 0 {
			cert, err := tls.LoadX509KeyPair(config.CertificateFile, config.KeyFile)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		} else {
			cert, err := config.NewSelfSignedCert()
			if err != nil {
				return nil, err
			}
			log.Printf("KeyFile not defined : server now use a self signed certificate")
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	clientMessageHandlers := proxy.NewClientMessageHandlers()
	serverMessageHandlers := proxy.NewServerMessageHandlers()

	clientMessageHandlers.AddHandleQuery(config.handleQuery)
	clientMessageHandlers.AddHandleParse(config.handleParse)
	clientMessageHandlers.AddHandleTerminate(config.handleTerminate)
	clientMessageHandlers.AddHandleBind(config.handleBind)
	serverMessageHandlers.AddHandleBackendKeyData(config.handleBackendKeyData)
	serverMessageHandlers.AddHandleParameterDescription(config.handleParameterDescription)
	serverMessageHandlers.AddHandleRowDescription(config.handleRowDescription)
	serverMessageHandlers.AddHandleReadyForQuery(config.handleReadyForQuery)
	serverMessageHandlers.AddHandleAuthenticationOk(config.handleAuthenticationOk)
	serverMessageHandlers.AddHandleErrorResponse(config.handleErrorResponse)
	serverMessageHandlers.AddHandleCopyInResponse(config.handleCopyInResponse)
	serverMessageHandlers.AddHandleCopyOutResponse(config.handleCopyOutResponse)
	serverMessageHandlers.AddHandleCopyData(config.handleCopyData)
	serverMessageHandlers.AddHandleCopyDone(config.handleCopyDone)

	if config.polyfillLock == nil {
		config.polyfillLock = &sync.RWMutex{}
	}
	if config.SqlTranslator == nil {
		config.SqlTranslator = IsoTranslator()
	}
	return &proxy.Server{
		TLSConfig:                tlsConfig,
		PGResolver:               config,
		PGStartupMessageRewriter: config,
		ConnInfoStore:            backend.NewInMemoryConnInfoStore(),
		ClientMessageHandlers:    clientMessageHandlers,
		ServerMessageHandlers:    serverMessageHandlers,
		OnHandleConnError:        config.handleConnError,
		ProtocolDebug:            config.Verbose&8 == 8,
	}, nil
}
