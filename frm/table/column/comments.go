package column

import (
	"fmt"

	"github.com/zing22845/go-frm-parser/frm/model"
)

type Comments struct {
	model.DataModel
	CurrentOffset uint32
	CommentsBytes [][]byte
}

// NewCommentsData creates a new Comments struct
// Offset: labels.Offset + labels.Length
// Length: fileInfo.COMMENTS_LENGTH
func NewCommentsData(data []byte, offset, length uint32) (c *Comments) {
	c = &Comments{}
	c.Offset = offset
	c.Length = length
	c.Data = data[c.Offset : c.Offset+c.Length]
	return c
}

func (c *Comments) Decode(length uint32, charsetName string) (comment string, err error) {
	if c.Length == 0 {
		return
	}
	if c.Length < c.CurrentOffset+length {
		return "",
			fmt.Errorf(
				"comments length(%d) is not enough(%d+%d)",
				c.Length, c.CurrentOffset, c.Length)
	}
	data := c.Data[c.CurrentOffset : c.CurrentOffset+length]
	c.CurrentOffset += length
	return string(data), nil
}
