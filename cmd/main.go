package main

import (
	"fmt"
	"os"

	"github.com/zing22845/go-frm-parser/frm"
)

func main() {
	// 打开 FRM 文件
	path := os.Args[1]
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// 调用 readAndParse 函数解析文件内容
	result, err := frm.Parse(path, file)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Printf("%s\n", result)
}
