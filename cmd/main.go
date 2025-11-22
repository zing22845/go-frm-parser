package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/zing22845/go-frm-parser/frm"
)

func main() {
	path := os.Args[1]
	file, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	buf := bytes.NewBuffer(file)

	// read and parse frm file
	result, err := frm.ParseBuffer(path, buf)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("====WITHOUT HEADER:\n%s", result.String())
	fmt.Printf("\n====WITH HEADER:\n%s", result.StringWithHeader())
}
