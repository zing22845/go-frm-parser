package table

import (
	"bytes"
	"fmt"
)

func Parse(path string, buf *bytes.Buffer) (*MySQLTable, error) {
	data := buf.Bytes()
	if len(data) < FILE_INFO_LENGTH {
		return nil, fmt.Errorf("%s is not a binary .frm file, size at least %d", path, FILE_INFO_LENGTH)
	}
	if !bytes.Equal(data[:2], []byte{0xfe, 0x01}) {
		return nil, fmt.Errorf("%s is not a binary .frm file", path)
	}

	fi, err := ReadFileInfo(path, data)
	if err != nil {
		return nil, err
	}

	err = fi.MySQLTable.Decode(data)
	if err != nil {
		return nil, err
	}
	return fi.MySQLTable, nil
}
