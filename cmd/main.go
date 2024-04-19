package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/zing22845/go-frm-parser/frm"
)

func main() {
	path := os.Args[1]
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, file)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// read and parse frm file
	result, err := frm.ParseBuffer(path, buf)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("====WITHOUT HEADER:\n%s", result.String())
	fmt.Printf("\n====WITH HEADER:\n%s", result.StringWithHeader())
}
