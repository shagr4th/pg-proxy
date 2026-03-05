package message

import (
	"bytes"
	"io"
)

type CopyData struct {
	Data         []byte
	BypassReturn bool
}

func (m *CopyData) Reader() io.Reader {
	if m.BypassReturn {
		return bytes.NewBuffer([]byte{})
	}
	b := NewBase(len(m.Data))
	b.WriteByteN(m.Data)
	return b.SetType('d').Reader()
}

func ReadCopyData(raw []byte) *CopyData {
	b := NewBaseFromBytes(raw)
	c := &CopyData{}
	c.Data = b.ReadByteN(uint32(len(raw)))
	return c
}
