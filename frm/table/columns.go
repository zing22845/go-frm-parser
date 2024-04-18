package table

import (
	"strings"

	"github.com/zing22845/go-frm-parser/frm/table/column"
)

type Columns struct {
	Count      uint16
	NullCount  uint16
	Metadata   *column.Metadata
	Names      *column.Names
	Labels     *column.Labels
	Comments   *column.Comments
	Defaults   *Defaults
	NullBitMap []byte
	NullBit    int
	Items      []*Column
	Combined   string
}

func (cs *Columns) Decode(table *MySQLTable) {
	cs.Names.Decode()
	cs.Labels.Decode()

	cs.NullBitMap = table.Defaults.Data[:(cs.NullCount+1+7)/8]
	table.Defaults.CurrentOffset += uint32((cs.NullCount + 1 + 7) / 8)
	cs.NullBit = 0
	if !table.Options.HandlerOptions.HasOption(HO_PACK_RECORD) {
		cs.NullBit = 1
	}

	cs.Items = make([]*Column, cs.Count)
	combined := make([]string, cs.Count)

	for fieldnr, name := range cs.Names.Items {
		column := &Column{
			Name:           name,
			Number:         fieldnr,
			TableCollation: table.Collation,
			NullBitMap:     cs.NullBitMap,
			NullBit:        cs.NullBit,
			Metadata:       cs.Metadata,
			Labels:         cs.Labels,
			Comments:       cs.Comments,
			Defaults:       table.Defaults,
		}
		cs.Items[fieldnr] = column
		column.Decode()
		combined[fieldnr] = "  " + column.String()
	}
	cs.Combined = strings.Join(combined, ",\n")
}
