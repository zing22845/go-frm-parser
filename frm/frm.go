package frm

import (
	"bytes"
	"fmt"
	"io"

	"github.com/zing22845/go-frm-parser/frm/table"
	"github.com/zing22845/go-frm-parser/frm/view"
)

type MySQLSchema interface {
	String() string
	StringWithHeader() string
}

func ParseBuffer(path string, buf *bytes.Buffer) (MySQLSchema, error) {
	header := buf.Bytes()[:9]
	if bytes.Equal(header[:2], []byte{0xfe, 0x01}) {
		return table.Parse(path, buf)
	} else if string(header) == "TYPE=VIEW" {
		return view.Parse(path, buf)
	} else {
		return nil, fmt.Errorf("invalid input format")
	}
}

func Parse(path string, r io.Reader) (MySQLSchema, error) {
	// Create a bytes.Buffer to store the entire input
	var buf bytes.Buffer

	// Read the first 9 bytes
	header := make([]byte, 9)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}

	// Write the first 9 bytes to the buffer
	_, err = buf.Write(header)
	if err != nil {
		return nil, err
	}

	// Check the header
	if bytes.Equal(header[:2], []byte{0xfe, 0x01}) {
		// Parse the input as a MySQL table
		_, err = io.Copy(&buf, r)
		if err != nil {
			return nil, err
		}
		return table.Parse(path, &buf)
	} else if string(header) == "TYPE=VIEW" {
		// Read the rest of the input and parse it as a MySQL view
		_, err = io.Copy(&buf, r)
		if err != nil {
			return nil, err
		}
		return view.Parse(path, &buf)
	} else {
		return nil, fmt.Errorf("invalid input format")
	}
}
