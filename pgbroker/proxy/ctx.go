package proxy

import (
	"context"
	"net"

	"schenker/pg-proxy/pgbroker/backend"
	"schenker/pg-proxy/pgbroker/message"
)

type AuthPhase int

const (
	PhaseStartup AuthPhase = iota
	PhaseGSS
	PhaseSASLInit
	PhaseSASL
	PhaseOK
)

type Ctx struct {
	ClientConn     net.Conn
	ServerConn     net.Conn
	ConnInfo       backend.ConnInfo
	RowDescription *message.RowDescription
	AuthPhase      AuthPhase
	Context        context.Context
	Cancel         context.CancelFunc
	QueryContext   any
}
