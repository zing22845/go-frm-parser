package main

import (
	"fmt"
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

	// read and parse frm file
	result, err := frm.Parse(path, file)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("%s\n", result)
}
