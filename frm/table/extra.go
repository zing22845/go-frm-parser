package table

import (
	"bytes"
	"encoding/binary"

	"github.com/zing22845/go-frm-parser/frm/model"
)

type Extra struct {
	model.DataModel
	CurrentOffset uint32
}

// NewExtraData creates a new Extra struct
// offset: fileInfo.DefaultsData.Offset + uint32(fileInfo._10_RECORD_LENGTH)
// length: fileInfo._37_EXTRA_INFO_LENGTH
func NewExtraData(data []byte, offset, length uint32) (e *Extra) {
	e = &Extra{}
	// read metadata offset, skip screens
	e.Offset = offset
	e.Length = length
	e.Data = data[e.Offset : e.Offset+e.Length]
	return e
}

func (e *Extra) DecodeParser() (result string) {
	data := e.Data[e.CurrentOffset:]
	// Find the index of the null terminator
	nullIdx := bytes.IndexByte(data, 0)
	if nullIdx == -1 {
		return ""
	}
	// Return the bytes up to the null terminator
	result = string(data[:nullIdx])
	e.CurrentOffset += uint32(nullIdx) + 1
	return result
}

func (e *Extra) DecodeTableComment() string {
	data := e.Data[e.CurrentOffset:]
	length := binary.LittleEndian.Uint16(data)
	return string(data[2 : 2+length])
}
