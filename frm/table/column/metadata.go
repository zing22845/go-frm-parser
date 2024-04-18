package column

import "github.com/zing22845/go-frm-parser/frm/model"

type Metadata struct {
	model.DataModel
	CurrentOffset uint32
}

// NewMetadata creates a new Metadata struct
// Offset: fileInfo.FORM_INFO_OFFSET + table.FORM_INFO_LENGTH + uint32(fileInfo.SCREENS_LENGTH)
// Length: 17 * uint32(fileInfo.COLUMN_COUNT)
func NewMetadata(data []byte, offset, length uint32) (md *Metadata) {
	md = &Metadata{}
	// read metadata offset, skip screens
	md.Offset = offset
	md.Length = length
	md.Data = data[md.Offset : md.Offset+md.Length]
	return md
}
