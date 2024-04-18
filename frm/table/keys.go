package table

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/zing22845/go-frm-parser/frm/model"
	"github.com/zing22845/go-frm-parser/frm/table/key"
)

type Keys struct {
	model.DataModel
	Extra         *Extra
	Count         uint8
	PartCount     uint16
	ExtraLength   uint16
	Items         []*Key
	CurrentOffset uint32
	Names         []string
	Comments      *key.Comments
	Combined      string
}

// NewKey
// offset: fileInfo._06_KEY_INFO_OFFSET
// length: fileInfo.KEY_INFO_LENGTH
func NewKeysData(data []byte, offset, length uint32) (k *Keys) {
	k = &Keys{}
	k.Offset = offset
	k.Length = length
	k.Data = data[k.Offset : k.Offset+k.Length]
	return k
}

const (
	BYTES_PER_KEY      = 8
	BYTES_PER_KEY_PART = 9
)

func (ks *Keys) Decode(columns *Columns) {
	ks.Count = uint8(ks.Data[0])
	if ks.Count < 128 {
		ks.PartCount = uint16(ks.Data[1])
	} else {
		ks.Count = (ks.Count & 0x7F) | (uint8(ks.Data[1]) << 7)
		ks.PartCount = binary.LittleEndian.Uint16(ks.Data[2:])
	}
	ks.ExtraLength = binary.LittleEndian.Uint16(ks.Data[4:])
	ks.CurrentOffset = 6
	// names, comments are calculated upfront so we can build the key as we go
	ks.Comments = &key.Comments{}
	ks.Names, ks.Comments.Data = ks.DecodeNamesComments()

	// decode key one by one
	ks.Items = make([]*Key, len(ks.Names))
	combined := make([]string, len(ks.Names))
	for i, name := range ks.Names {
		key := &Key{
			Name:    name,
			Keys:    ks,
			Columns: columns,
		}
		ks.Items[i] = key
		key.Decode()
		combined[i] = "  " + key.String()
	}
	ks.Combined = strings.Join(combined, ",\n")
}

func (k *Keys) DecodeNamesComments() (names []string, comments []byte) {
	extraOffset := k.CurrentOffset +
		uint32(k.Count)*BYTES_PER_KEY +
		uint32(k.PartCount)*BYTES_PER_KEY_PART
	extraInfo := k.Data[extraOffset : extraOffset+uint32(k.ExtraLength)]
	// Split the input on the first null byte to separate names from comments
	parts := bytes.SplitN(extraInfo, []byte{0x00}, 2)
	namesPart := parts[0]
	if len(parts) > 1 {
		comments = parts[1]
	}
	// Split the names part on 0xFF and decode each part from UTF-8
	namesBytes := bytes.Split(bytes.Trim(namesPart, "\xFF"), []byte{0xFF})
	names = make([]string, len(namesBytes))
	for i, name := range namesBytes {
		names[i] = string(name)
	}
	return names, comments
}
