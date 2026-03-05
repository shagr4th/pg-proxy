package message

import (
	"bytes"
	"io"
)

type CopyOutResponse struct {
	OverallFormat     uint8
	ColumnFormatCodes []uint16
	BypassReturn      bool
}

func (m *CopyOutResponse) Reader() io.Reader {
	if m.BypassReturn {
		return bytes.NewBuffer([]byte{})
	}
	b := NewBase(1 + 2 + 2*len(m.ColumnFormatCodes))
	b.WriteUint8(m.OverallFormat)
	b.WriteUint16(uint16(len(m.ColumnFormatCodes)))
	for _, c := range m.ColumnFormatCodes {
		b.WriteUint16(c)
	}
	return b.SetType('H').Reader()
}

func ReadCopyOutResponse(raw []byte) *CopyOutResponse {
	b := NewBaseFromBytes(raw)
	resp := &CopyOutResponse{}
	resp.OverallFormat = b.ReadUint8()
	resp.ColumnFormatCodes = make([]uint16, b.ReadUint16())
	for i := 0; i < len(resp.ColumnFormatCodes); i++ {
		resp.ColumnFormatCodes[i] = b.ReadUint16()
	}
	return resp
}
