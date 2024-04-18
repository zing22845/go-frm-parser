package table

import (
	"encoding/binary"
	"fmt"
)

type FormInfo struct {
	FORM_INFO_OFFSET     uint32
	SCREENS_LENGTH       uint16
	COLUMN_COUNT         uint16
	NULL_FIELDS          uint16
	NAMES_LENGTH         uint16
	LABELS_LENGTH        uint16
	COMMENTS_LENGTH      uint16
	TABLE_COMMENT_LENGTH uint8
}

func ReadFormInfo(data []byte, fileInfo *FileInfo) (fi *FormInfo, err error) {
	fi = &FormInfo{}
	// read form info offset
	formInfoOffsetStart := FILE_INFO_LENGTH + fileInfo._04_NAMES_LENGTH
	formInfoOffsetEnd := formInfoOffsetStart + 4
	if len(data) < int(formInfoOffsetEnd) {
		return nil, fmt.Errorf("data is too short to read form info offset: %d", formInfoOffsetEnd)
	}
	fi.FORM_INFO_OFFSET = binary.LittleEndian.Uint32(
		data[formInfoOffsetStart:formInfoOffsetEnd])
	// check form info
	if len(data) < int(fi.FORM_INFO_OFFSET+FORM_INFO_LENGTH) {
		return nil, fmt.Errorf("data is too short to read form info: %d", fi.FORM_INFO_OFFSET+FORM_INFO_LENGTH)
	}
	fi.SCREENS_LENGTH = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+260 : fi.FORM_INFO_OFFSET+262])
	// Column
	fi.COLUMN_COUNT = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+258 : fi.FORM_INFO_OFFSET+260])
	fi.NAMES_LENGTH = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+268 : fi.FORM_INFO_OFFSET+270])
	fi.NULL_FIELDS = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+282 : fi.FORM_INFO_OFFSET+284])
	fi.LABELS_LENGTH = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+274 : fi.FORM_INFO_OFFSET+276])
	fi.COMMENTS_LENGTH = binary.LittleEndian.Uint16(data[fi.FORM_INFO_OFFSET+284 : fi.FORM_INFO_OFFSET+286])
	// table comment
	fi.TABLE_COMMENT_LENGTH = uint8(data[fi.FORM_INFO_OFFSET+46])
	return fi, nil
}
