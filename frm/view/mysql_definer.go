package view

import "fmt"

type MySQLDefiner struct {
	User string
	Host string
}

func (d *MySQLDefiner) String() string {
	return fmt.Sprintf("`%s`@`%s`", d.User, d.Host)
}
