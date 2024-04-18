package table

import (
	"github.com/zing22845/go-frm-parser/frm/model"
)

type Defaults struct {
	model.DataModel
	CurrentOffset uint32
}

// NewDefaults
// offset: uint32(fileInfo._06_KEY_INFO_OFFSET) + fileInfo.KEY_INFO_LENGTH
// length: uint32(fileInfo._10_RECORD_LENGTH)
func NewDefaultsData(data []byte, offset, length uint32) (d *Defaults) {
	d = &Defaults{}
	d.Offset = offset
	d.Length = length
	d.Data = data[d.Offset : d.Offset+d.Length]
	return d
}
