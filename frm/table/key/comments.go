package key

import (
	"encoding/binary"
)

type Comments struct {
	Data          []byte
	CurrentOffset uint32
}

func NewCommentsData(data []byte) (c *Comments) {
	return &Comments{Data: data}
}

func (c *Comments) Decode() (comment string) {
	if len(c.Data) == 0 {
		return
	}
	data := c.Data[c.CurrentOffset:]
	length := binary.LittleEndian.Uint16(data)
	comment = string(data[2 : 2+length])
	c.CurrentOffset += 2 + uint32(length)
	return comment
}
