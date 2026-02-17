package main

import (
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
	"strings"
	"sync"
	"time"

	"github.com/rueian/pgbroker/backend"
	"github.com/rueian/pgbroker/message"
	"github.com/rueian/pgbroker/proxy"
)

type ProxyConfig struct {
	Host              string
	Port              int
	Remote            string
	CertificateFile   string
	KeyFile           string
	Verbose           int
	Polyfilled        bool
	StartupParameters map[string]string
	SqlTranslator
	polyfillLock     *sync.RWMutex
	latestQueryError sync.Map
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
	if config.StartupParameters != nil {
		maps.Copy(original, config.StartupParameters)
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
	parsed, err := config.Translate(msg.QueryString, config.Polyfilled, false)
	if err != nil {
		config.latestQueryError.Store(ctx, err)
		return msg, nil
	}
	if parsed == nil || !parsed.Transformed {
		if config.Verbose&4 == 4 {
			log.Printf("INFO  [%s] %s\n", config.clientInfo(ctx), msg.QueryString)
		}
		return msg, nil
	}
	msg.QueryString = parsed.Sql() + " --translated from:\n-- " +
		strings.ReplaceAll(strings.ReplaceAll(msg.QueryString, "\n", "\n-- "), "\r", "")
	if config.Verbose&2 == 2 {
		log.Printf("INFO  [%s] %s in %d µs\n", config.clientInfo(ctx), msg.QueryString, time.Since(start).Microseconds())
	}
	return msg, nil
}

func (config *ProxyConfig) handleQuery(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
	parseMsg, _ := config.handleParse(ctx, &message.Parse{
		QueryString: msg.QueryString,
	})
	if parseMsg != nil {
		msg.QueryString = parseMsg.QueryString
	}
	return msg, nil
}

func (config *ProxyConfig) sendQueryToServer(ctx *proxy.Ctx, query string) error {
	// Send the query to the backend server
	queryMsg := &message.Query{QueryString: query}
	if _, err := io.Copy(ctx.ServerConn, queryMsg.Reader()); err != nil {
		return fmt.Errorf("Polyfill write failed: %w", err)
	}

	var result error
	// Read and discard all response messages until the next ReadyForQuery ('Z')
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(ctx.ServerConn, header); err != nil {
			return fmt.Errorf("Polyfill read failed: %w", err)
		}

		msgType := header[0]
		bodyLen := int(binary.BigEndian.Uint32(header[1:5])) - 4

		var body []byte
		if bodyLen > 0 {
			body = make([]byte, bodyLen)
			if _, err := io.ReadFull(ctx.ServerConn, body); err != nil {
				return fmt.Errorf("Polyfill read failed: %w", err)
			}
		}

		if msgType == 'E' {
			result = errors.New("Polyfill error")
			errResp := message.ReadErrorResponse(body)
			for _, f := range errResp.Fields {
				if f.Type == 'M' {
					result = errors.New(f.Value)
					break
				}
			}
		}

		if msgType == 'Z' {
			return result
		}
	}
}

func (config *ProxyConfig) handleReadyForQuery(ctx *proxy.Ctx, msg *message.ReadyForQuery) (*message.ReadyForQuery, error) {
	config.latestQueryError.Delete(ctx)
	if config.Polyfilled {
		return msg, nil
	}
	config.polyfillLock.Lock()
	start := time.Now()
	checkPolyfill, createPolyfill := config.SqlTranslator.Polyfill()
	if createPolyfill == "" {
		return msg, nil
	}
	err := config.sendQueryToServer(ctx, checkPolyfill)
	if err != nil { // seems polyfill is not installed
		err = config.sendQueryToServer(ctx, createPolyfill)
		if config.Verbose&1 == 1 {
			if err == nil {
				log.Printf("INFO  [%s] Executed polyfill in %d µs\n", config.clientInfo(ctx), time.Since(start).Microseconds())
			} else {
				log.Printf("ERROR [%s] Polyfill error: %s\n", config.clientInfo(ctx), err.Error())
			}
		}
	}
	config.Polyfilled = true
	config.polyfillLock.Unlock()
	return msg, nil
}

func (config *ProxyConfig) handleTerminate(ctx *proxy.Ctx, msg *message.Terminate) (*message.Terminate, error) {
	config.latestQueryError.Delete(ctx)
	if config.Verbose&1 == 1 {
		log.Printf("INFO  [%s] Client sent termination\n", config.clientInfo(ctx))
	}
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
		name, err := config.RenameColumn(i, msg.Fields[i].Name)
		if err != nil {
			config.latestQueryError.Store(ctx, err)
			return msg, nil
		}
		msg.Fields[i].Name = name
	}
	ctx.RowDescription = msg
	return msg, nil
}

func (config *ProxyConfig) handleErrorResponse(ctx *proxy.Ctx, msg *message.ErrorResponse) (*message.ErrorResponse, error) {
	queryErrorObject, queryErrorLoaded := config.latestQueryError.LoadAndDelete(ctx)
	if config.Verbose == 0 && !queryErrorLoaded {
		return msg, nil
	}
	var errorMessage string
	var errorCode string
	for i, err := range msg.Fields {
		switch err.Type {
		case 77:
			errorMessage = err.Value
			if queryErrorLoaded {
				queryError, ok := queryErrorObject.(error)
				if ok {
					errorMessage = fmt.Sprintf("pg-proxy error: %v (postgres error: %s)", queryError, errorMessage)
					newFields := append(msg.Fields[0:i], message.ErrorField{
						Type:  err.Type,
						Value: errorMessage,
					})
					msg.Fields = append(newFields, msg.Fields[i+1:]...)
				}
			}
		case 67:
			errorCode = err.Value
		}
	}
	if (config.Verbose&2 == 2 || config.Verbose&4 == 4) && len(errorMessage) > 0 {
		log.Printf("ERROR [%s] %s (%s)\n", config.clientInfo(ctx), errorMessage, errorCode)
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

	if config.SqlTranslator != nil {
		clientMessageHandlers.AddHandleQuery(config.handleQuery)
		clientMessageHandlers.AddHandleParse(config.handleParse)
		serverMessageHandlers.AddHandleRowDescription(config.handleRowDescription)
	}

	clientMessageHandlers.AddHandleTerminate(config.handleTerminate)
	serverMessageHandlers.AddHandleAuthenticationOk(config.handleAuthenticationOk)
	serverMessageHandlers.AddHandleErrorResponse(config.handleErrorResponse)
	serverMessageHandlers.AddHandleReadyForQuery(config.handleReadyForQuery)

	if config.polyfillLock == nil {
		config.polyfillLock = &sync.RWMutex{}
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
