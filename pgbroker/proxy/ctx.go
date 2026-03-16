package proxy

import (
	"context"
	"net"

	"schenker/pg-proxy/pgbroker/backend"
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
	ClientConn   net.Conn
	ServerConn   net.Conn
	ConnInfo     backend.ConnInfo
	AuthPhase    AuthPhase
	Context      context.Context
	Cancel       context.CancelFunc
	QueryContext any
}
