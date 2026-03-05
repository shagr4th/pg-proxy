package proxy

import (
	"schenker/pg-proxy/pgbroker/message"
)

type MessageHandler func(*Ctx, []byte) (message.Reader, error)

type MessageHandlerRegister interface {
	GetHandler(byte) MessageHandler
}
