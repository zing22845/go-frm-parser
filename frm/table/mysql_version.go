package table

import (
	"encoding/binary"
	"fmt"
)

type MySQLVersion struct {
	Major, Minor, Release int
}

func NewMySQLVersion(data []byte) (mv *MySQLVersion) {
	mv = new(MySQLVersion)
	versionID := binary.LittleEndian.Uint32(data)
	mv.Major = int(versionID / 10000)
	mv.Minor = int(versionID % 1000 / 100)
	mv.Release = int(versionID % 100)
	return mv
}

func (mv *MySQLVersion) String() string {
	if mv.Major == 0 && mv.Minor == 0 && mv.Release == 0 {
		return "< 5.0"
	}
	return fmt.Sprintf("%d.%d.%d", mv.Major, mv.Minor, mv.Release)
}
