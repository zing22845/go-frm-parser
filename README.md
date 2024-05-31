# go-frm-parser

Parsing MySQL '.frm' file from stream inspired by frmdump of dbsake

## Why we write this

We have a MySQL database has lots of tables and we need the consistency table schema of the Xtrabackup result.
When streaming to storage, we managed to get the schema info from the physical files(frm for MySQL 5.x, ibd for 8.x)
The dbsake only takes frm data from a file, this lib can take it from memory.

## How is the compatability

It's goal is totally compatable with `dbsake frmdump tablename.frm` result.

## How is the performance

We tested the performance with MySQL 5.7.38 and the speed is about 30x faster than dbsake version.
Example:
A database which has 229 tables takes 30s by using dbsake, but 1.29s by using go-frm-parser

## How to use it

Just give the parser filepath and file reader of the frm file.

```go
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
```
