package view

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type MySQLView struct {
	Name        string
	Algorithm   Algorithm
	Definer     MySQLDefiner
	SUID        SUIDType
	Body        string
	CheckOption CheckOption
	StoredMD5   string
	ComputedMD5 string
	Timestamp   time.Time
}

func (v *MySQLView) GetName() string {
	return v.Name
}

func (v *MySQLView) ParseName(path string) {
	v.Name = strings.TrimSuffix(filepath.Base(path), ".frm")
}

func (v *MySQLView) String() string {
	parts := make([]string, 0, 10)
	parts = append(parts, "CREATE")

	parts = append(parts, fmt.Sprintf("ALGORITHM=%s", v.Algorithm.String()))
	parts = append(parts, fmt.Sprintf("DEFINER=%s", v.Definer.String()))

	security := "DEFINER"
	if v.SUID.String() != "DEFAULT" {
		security = v.SUID.String()
	}
	parts = append(parts, "SQL SECURITY "+security)

	parts = append(parts, "VIEW")
	parts = append(parts, fmt.Sprintf("`%s`", v.Name))
	parts = append(parts, "AS")
	parts = append(parts, v.Body)

	if v.CheckOption != None {
		parts = append(parts, "WITH "+v.CheckOption.String()+" CHECK OPTION")
	}

	return strings.Join(parts, " ") + ";\n"
}

func (v *MySQLView) StringWithHeader() string {
	header := strings.Join([]string{
		"--",
		fmt.Sprintf("-- View: %s", v.Name),
		fmt.Sprintf("-- Timestamp: %s", v.Timestamp.Format("2006-01-02 15:04:05")),
		fmt.Sprintf("-- Stored MD5: %s", v.StoredMD5),
		fmt.Sprintf("-- Computed MD5: %s", v.ComputedMD5),
		"--",
		"",
		"",
	}, "\n")

	return header + v.String()
}
