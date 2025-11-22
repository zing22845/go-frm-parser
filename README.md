# go-frm-parser

go-frm-parser is a Golang library for parsing MySQL `.frm` files, designed to extract table schema information from both memory and disk files. It is inspired by the `frmdump` functionality of [dbsake](https://github.com/abg/dbsake), but provides additional features, improved performance, and flexibility in data sources.

## Why we wrote this

We developed go-frm-parser to address a specific need in our MySQL database backup and restoration process. Our database contains a large number of tables, and we require a way to ensure consistency in table schemas when working with Xtrabackup results.

During the backup process, when streaming data to storage, we extract schema information from the physical files (`.frm` files for MySQL 5.x and `.ibd` files for MySQL 8.x). While dbsake's `frmdump` can extract schema information from `.frm` files on disk, it does not support reading `.frm` data from memory. 

go-frm-parser bridges this gap by providing the ability to parse `.frm` data from both memory and disk files, giving us the flexibility to extract schema information in various scenarios.

## Features

- Parse `.frm` data from both memory and disk files
- Support for MySQL `.frm` file formats
- High-performance parsing, with 5x-10x faster parsing speed compared to `dbsake`
- Provides a simple, intuitive API for easy integration into your Golang projects
- Supports parsing of `.frm` files from MySQL 5.x

## Installation

To install go-frm-parser, simply run:

```
go get github.com/zing22845/go-frm-parser
```

## Usage

### Parse from file

Here's a basic example of how to use go-frm-parser:

```go
package main

import (
    "fmt"
    "os"
    frm "github.com/zing22845/go-frm-parser/frm"
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
    fmt.Printf("====WITHOUT HEADER:\n%s", result.String())
    fmt.Printf("\n====WITH HEADER:\n%s", result.StringWithHeader())
}
```

### Parsing from Buffer

You can also parse `.frm` data directly from a byte slice (e.g., read from a network stream or memory):

```go
package main

import (
    "bytes"
    "fmt"
    "os"
    frm "github.com/zing22845/go-frm-parser/frm"
)

func main() {
    path := os.Args[1]
    // Simulate reading data into a buffer
    data, err := os.ReadFile(path)
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    
    buf := bytes.NewBuffer(data)

    // Parse from buffer
    result, err := frm.ParseBuffer(path, buf)
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    fmt.Printf("====WITHOUT HEADER:\n%s", result.String())
}
```

## Comparison with dbsake

go-frm-parser provides several advantages over the `frmdump` functionality in dbsake:

| Feature | go-frm-parser | dbsake |
|---------|---------------|--------|
| Language | Go | Python |
| Parses `.frm` files from disk | ✅ | ✅ |
| Parses `.frm` data from memory | ✅ | ❌ |
| Handles `decimal(1,1)` | ✅ | ❌ |
| Supports `datetime NOT NULL DEFAULT '0000-00-00 00:00:00'` | ✅ | ❌ |
| Handles `enum` column types | ✅ | ❌ |
| Parsing speed | 5x-10x faster | 1x |

While dbsake's `frmdump` is a useful tool, go-frm-parser provides a more comprehensive, performant, and flexible solution for parsing MySQL `.frm` files and data.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

go-frm-parser is released under the [MIT License](LICENSE).
