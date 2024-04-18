package column

import (
	"bytes"

	"github.com/zing22845/go-frm-parser/frm/model"
)

type Names struct {
	model.DataModel
	Items []string
}

// NewNamesData creates a new Names struct
// Offset: metadata.End
// Length: fileInfo.NAMES_LENGTH
func NewNamesData(data []byte, offset, length uint32) (n *Names) {
	n = &Names{}
	n.Offset = offset
	n.Length = length
	n.Data = data[n.Offset : n.Offset+n.Length]
	return n
}

// Decode Names.Data into Names.Items
func (n *Names) Decode() {
	byteItems := bytes.Split(n.Data[1:len(n.Data)-2], []byte{0xFF})
	n.Items = make([]string, len(byteItems))
	for i, name := range byteItems {
		n.Items[i] = string(name)
	}
}
