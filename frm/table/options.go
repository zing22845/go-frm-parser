package table

import (
	"fmt"
	"strings"
)

type Options struct {
	Connection     string
	Engine         string
	Collation      *Collation
	MinRows        uint32
	MaxRows        uint32
	AvgRowLength   uint32
	RowFormat      RowType
	KeyBlockSize   uint16
	Comment        string
	Partitions     string
	HandlerOptions HandlerOption
}

func (t *Options) String() string {
	var parts []string
	if t.Connection != "" {
		parts = append(parts, fmt.Sprintf("CONNECTION='%s'", t.Connection))
	}
	if t.Engine != "" {
		parts = append(parts, fmt.Sprintf("ENGINE=%s", t.Engine))
	}
	if t.Collation != nil && t.Collation.Name != "" {
		parts = append(parts, fmt.Sprintf("DEFAULT CHARSET=%s", t.Collation.CharsetName))
		if !t.Collation.IsDefault {
			parts = append(parts, fmt.Sprintf("COLLATE=%s", t.Collation.Name))
		}
	}
	if t.MinRows != 0 {
		parts = append(parts, fmt.Sprintf("MIN_ROWS=%d", t.MinRows))
	}
	if t.MaxRows != 0 {
		parts = append(parts, fmt.Sprintf("MAX_ROWS=%d", t.MaxRows))
	}
	if t.AvgRowLength != 0 {
		parts = append(parts, fmt.Sprintf("AVG_ROW_LENGTH=%d", t.AvgRowLength))
	}
	if t.KeyBlockSize != 0 {
		parts = append(parts, fmt.Sprintf("KEY_BLOCK_SIZE=%d", t.KeyBlockSize))
	}
	if t.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT='%s'", t.Comment))
	}
	if t.Partitions != "" {
		parts = append(parts, fmt.Sprintf("/*!50100 %s */", t.Partitions))
	}
	return strings.Join(parts, " ")
}

func NewOptions(fileInfo *FileInfo) (o *Options) {
	o = &Options{}
	o.MaxRows = fileInfo._12_MAX_ROWS
	o.MinRows = fileInfo._16_MIN_ROWS
	o.AvgRowLength = fileInfo._22_AVG_ROW_LENGTH
	o.RowFormat = fileInfo._28_ROW_TYPE
	o.KeyBlockSize = fileInfo._3E_KEY_BLOCK_SIZE
	o.HandlerOptions = fileInfo._1E_HANDLER_OPTION
	return o
}
