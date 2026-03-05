package message

import (
	"bytes"
	"io"
)

type CopyDone struct {
	BypassReturn bool
}

func (m *CopyDone) Reader() io.Reader {
	if m.BypassReturn {
		return bytes.NewBuffer([]byte{})
	}
	b := NewBase(0)
	return b.SetType('c').Reader()
}

func ReadCopyDone(raw []byte) *CopyDone {
	return &CopyDone{}
}
