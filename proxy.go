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
	Polyfilled                bool
	StartupParametersOverride map[string]string
	QueryStore                *QueryStore
	SqlTranslator
	polyfillLock *sync.RWMutex
}

var _ backend.PGStartupMessageRewriter = (*ProxyConfig)(nil)
var _ backend.PGResolver = (*ProxyConfig)(nil)

const (
	proxyOriginalKey         string = "_proxy_original"
	proxyTranslationKey      string = "_proxy_translation"
	proxyCopyFromKey         string = "_proxy_copyfrom"
	proxyCopyFromExtendedKey string = "_proxy_copyfrome"
	proxyCopyToKey           string = "_proxy_copyto"
	proxyErrorKey            string = "_proxy_error"
	proxyTimeKey             string = "_proxy_time"
)

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

func (config *ProxyConfig) handleConnError(err error, ctx *proxy.Ctx, conn net.Conn) {
	if err != io.EOF && config.Verbose&1 == 1 {
		log.Printf("WARN  [%s] Connection error : %v", config.clientInfo(ctx), err)
	}
}

func (config *ProxyConfig) clientInfo(ctx *proxy.Ctx) string {
	return fmt.Sprintf("%-5d %-40s", ctx.ConnInfo.BackendProcessID, fmt.Sprintf("%v (%v)", ctx.ConnInfo.StartupParameters["user"],
		ctx.ConnInfo.ClientAddress))
}

func (config *ProxyConfig) handleParse(ctx *proxy.Ctx, msg *message.Parse) (parse *message.Parse, e error) {
	start := time.Now()
	if config.QueryStore != nil {
		ctx.ConnInfo.StartupParameters[proxyTimeKey] = start.Format(time.RFC3339)
		ctx.ConnInfo.StartupParameters[proxyTranslationKey] = ""
	}
	ctx.ConnInfo.StartupParameters[proxyOriginalKey] = msg.QueryString
	parsed, err := config.Translate(msg.QueryString, config.Polyfilled, false)
	if err != nil {
		ctx.ConnInfo.StartupParameters[proxyErrorKey] = err.Error()
	}
	if parsed != nil {
		ctx.ConnInfo.StartupParameters[proxyCopyFromKey] = parsed.CopyFrom
		if msg.PreparedStatementName != "dummyQuery" {
			ctx.ConnInfo.StartupParameters[proxyCopyFromExtendedKey] = parsed.CopyFrom
		}
		ctx.ConnInfo.StartupParameters[proxyCopyToKey] = parsed.CopyTo
	}
	if parsed == nil || !parsed.Transformed {
		if config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] %s\n", config.clientInfo(ctx), msg.QueryString)
		}
		return msg, nil
	}
	msg.QueryString = parsed.Sql()
	if config.QueryStore != nil {
		ctx.ConnInfo.StartupParameters[proxyTranslationKey] = msg.QueryString
	}
	if config.KeepOriginal {
		msg.QueryString += " --translated from:\n-- " + strings.ReplaceAll(strings.ReplaceAll(msg.QueryString, "\n", "\n-- "), "\r", "")
	}
	if config.Verbose&2 == 2 {
		log.Printf("INFO  [%s] %s in %d µs\n", config.clientInfo(ctx), msg.QueryString, time.Since(start).Microseconds())
	}
	return msg, nil
}

func (config *ProxyConfig) handleQuery(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
	parseMsg, _ := config.handleParse(ctx, &message.Parse{
		PreparedStatementName: "dummyQuery",
		QueryString:           msg.QueryString,
	})
	if parseMsg != nil {
		msg.QueryString = parseMsg.QueryString
	}
	return msg, nil
}

func (config *ProxyConfig) sendDataToServer(ctx *proxy.Ctx, reader io.Reader, finalExpectedType byte) ([]byte, error) {
	if _, err := io.Copy(ctx.ServerConn, reader); err != nil {
		return nil, fmt.Errorf("Polyfill write failed: %w", err)
	}

	var result error
	// Read and discard all response messages until the next ReadyForQuery ('Z')
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
	originalQuery, originalQueryFound := ctx.ConnInfo.StartupParameters[proxyOriginalKey]
	if originalQueryFound {
		if config.QueryStore != nil {
			start, hasStart := ctx.ConnInfo.StartupParameters[proxyTimeKey]
			duration := ""
			if hasStart && start != "" {
				startTime, err := time.Parse(time.RFC3339, start)
				if err == nil {
					millis := time.Since(startTime).Milliseconds()
					duration = fmt.Sprint(millis)
				}
			}
			config.QueryStore.Add(QueryRecord{
				Time:        start,
				Duration:    duration,
				ClientInfo:  config.clientInfo(ctx),
				OriginalSQL: originalQuery,
				FinalSQL:    ctx.ConnInfo.StartupParameters[proxyTranslationKey],
				Error:       ctx.ConnInfo.StartupParameters[proxyErrorKey],
			})
		}
		delete(ctx.ConnInfo.StartupParameters, proxyOriginalKey)
		delete(ctx.ConnInfo.StartupParameters, proxyTimeKey)
		delete(ctx.ConnInfo.StartupParameters, proxyTranslationKey)
	}
	delete(ctx.ConnInfo.StartupParameters, proxyErrorKey)
	delete(ctx.ConnInfo.StartupParameters, proxyCopyToKey)
}

func (config *ProxyConfig) managePolyfill(ctx *proxy.Ctx) {
	if config.Polyfilled {
		return
	}
	config.polyfillLock.Lock()
	defer config.polyfillLock.Unlock()
	start := time.Now()
	checkPolyfill, createPolyfill := config.Polyfill()
	if createPolyfill == "" {
		config.Polyfilled = true
		return
	}
	_, err := config.sendDataToServer(ctx, (&message.Query{QueryString: checkPolyfill}).Reader(), 'Z')
	if err != nil { // seems polyfill is not installed
		_, err = config.sendDataToServer(ctx, (&message.Query{QueryString: createPolyfill}).Reader(), 'Z')
		if config.Verbose&1 == 1 {
			if err == nil {
				log.Printf("INFO  [%s] Executed polyfill in %d µs\n", config.clientInfo(ctx), time.Since(start).Microseconds())
			} else {
				log.Printf("ERROR [%s] Polyfill error: %s\n", config.clientInfo(ctx), err.Error())
			}
		}
	}
	config.Polyfilled = true
}

func (config *ProxyConfig) handleReadyForQuery(ctx *proxy.Ctx, msg *message.ReadyForQuery) (*message.ReadyForQuery, error) {
	config.cleanupStore(ctx)
	config.managePolyfill(ctx)
	return msg, nil
}

func (config *ProxyConfig) handleTerminate(ctx *proxy.Ctx, msg *message.Terminate) (*message.Terminate, error) {
	if config.Verbose&1 == 1 {
		log.Printf("INFO  [%s] Client sent termination\n", config.clientInfo(ctx))
	}
	config.cleanupStore(ctx)
	return msg, nil
}

func (config *ProxyConfig) handleAuthenticationOk(ctx *proxy.Ctx, msg *message.AuthenticationOk) (*message.AuthenticationOk, error) {
	if config.Verbose&1 == 1 {
		log.Printf("INFO  [%s] Server authentification OK (%d)\n", config.clientInfo(ctx), msg.ID)
	}
	return msg, nil
}

func (config *ProxyConfig) handleRowDescription(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
	for i := range msg.Fields {
		msg.Fields[i].Name = config.RenameColumn(i, msg.Fields[i].Name)
	}
	ctx.RowDescription = msg
	return msg, nil
}

func (config *ProxyConfig) handleCopyInResponse(ctx *proxy.Ctx, msg *message.CopyInResponse) (*message.CopyInResponse, error) {
	copyFrom, ok := ctx.ConnInfo.StartupParameters[proxyCopyFromKey]
	copyExtendedFrom, okExtended := ctx.ConnInfo.StartupParameters[proxyCopyFromExtendedKey]
	if ok && config.IsCopyLocal() {
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy from %s\n", config.clientInfo(ctx), copyFrom)
		}
		f, err := os.Open(copyFrom)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		buf := make([]byte, 65536)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
					log.Printf("INFO  [%s] Read %d bytes from %s\n", config.clientInfo(ctx), n, copyFrom)
				}
				chunk := buf[:n]
				if _, err := io.Copy(ctx.ServerConn, message.ReadCopyData(chunk).Reader()); err != nil {
					return nil, fmt.Errorf("Copy Data write failed: %w", err)
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
			return nil, fmt.Errorf("Copy Done write failed: %w", err)
		}
		if okExtended && copyExtendedFrom == copyFrom {
			if _, err := io.Copy(ctx.ServerConn, message.ReadSync([]byte{}).Reader()); err != nil {
				return nil, fmt.Errorf("Copy Done write failed: %w", err)
			}
		}
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy from %s done\n", config.clientInfo(ctx), copyFrom)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleCopyOutResponse(ctx *proxy.Ctx, msg *message.CopyOutResponse) (*message.CopyOutResponse, error) {
	copyTo, ok := ctx.ConnInfo.StartupParameters[proxyCopyToKey]
	if ok && config.IsCopyLocal() {
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Will copy to %s\n", config.clientInfo(ctx), copyTo)
		}
		f, err := os.Create(copyTo)
		if err != nil {
			return msg, err
		}
		defer f.Close()
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleCopyData(ctx *proxy.Ctx, msg *message.CopyData) (*message.CopyData, error) {
	copyTo, ok := ctx.ConnInfo.StartupParameters[proxyCopyToKey]
	if ok && config.IsCopyLocal() {
		f, err := os.OpenFile(copyTo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
	copyTo, ok := ctx.ConnInfo.StartupParameters[proxyCopyToKey]
	if ok && config.IsCopyLocal() {
		delete(ctx.ConnInfo.StartupParameters, proxyCopyFromKey)
		delete(ctx.ConnInfo.StartupParameters, proxyCopyFromExtendedKey)
		if config.Verbose&2 == 2 || config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] Copy to %s done\n", config.clientInfo(ctx), copyTo)
		}
		msg.BypassReturn = true
	}
	return msg, nil
}

func (config *ProxyConfig) handleErrorResponse(ctx *proxy.Ctx, msg *message.ErrorResponse) (*message.ErrorResponse, error) {
	var errorMessage string
	var errorCode string
	for i, err := range msg.Fields {
		switch err.Type {
		case 77:
			errorMessage = err.Value
			query, queryFound := ctx.ConnInfo.StartupParameters[proxyOriginalKey]
			queryError, errorFound := ctx.ConnInfo.StartupParameters[proxyErrorKey]
			if queryFound && !errorFound { // erreur postgres sans traduction en erreur
				ctx.ConnInfo.StartupParameters[proxyErrorKey] = err.Value
				errorMessage = fmt.Sprintf("%v (from query: %s)", err.Value, query)
			} else if errorFound { // erreur interne du proxy
				if !queryFound {
					query = "<unknown>"
				}
				errorMessage = fmt.Sprintf("pg-proxy error: %v (from query: %s) (postgres error: %s)", queryError, query, err.Value)
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
	delete(ctx.ConnInfo.StartupParameters, proxyCopyFromKey)
	delete(ctx.ConnInfo.StartupParameters, proxyCopyFromExtendedKey)
	config.cleanupStore(ctx)
	if (config.Verbose&2 == 2 || config.Verbose&4 == 4) && len(errorMessage) > 0 {
		log.Printf("ERROR [%s] %s (error code: %s)\n", config.clientInfo(ctx), errorMessage, errorCode)
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
	}, nil
}
