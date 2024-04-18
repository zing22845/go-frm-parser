package column

import (
	"bytes"

	"github.com/zing22845/go-frm-parser/frm/model"
)

// Labels only for ENUM or SET columns
type Labels struct {
	model.DataModel
	Items [][][]byte
}

// NewLabelsData creates a new Labels struct
// Offset: names.Offset + names.Length
// Length: fileInfo.LABELS_LENGTH
func NewLabelsData(data []byte, offset, length uint32) (l *Labels) {
	l = &Labels{}
	l.Offset = offset
	l.Length = length
	l.Data = data[l.Offset : l.Offset+l.Length]
	return l
}

func (l *Labels) Decode() {
	if l.Length == 0 {
		return
	}
	labelGroups := bytes.Split(l.Data[:l.Length-1], []byte{0x00})
	l.Items = make([][][]byte, len(labelGroups))

	for n, group := range labelGroups {
		names := make([][]byte, bytes.Count(group, []byte{0xFF})-1)
		for m, name := range bytes.Split(group[1:len(group)-1], []byte{0xFF}) {
			names[m] = name
		}
		l.Items[n] = names
	}
}
