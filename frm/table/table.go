package table

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/zing22845/go-frm-parser/frm/table/column"
	"github.com/zing22845/go-frm-parser/frm/utils"
)

type MySQLTable struct {
	FileInfo     *FileInfo
	Name         string
	MySQLVersion *MySQLVersion
	Keys         *Keys
	Defaults     *Defaults
	Extra        *Extra
	Columns      *Columns
	Collation    *Collation
	Options      *Options
}

func NewMySQLTable(path string, data []byte, fi *FileInfo) (mt *MySQLTable, err error) {
	mt = &MySQLTable{}
	mt.FileInfo = fi
	// parse table name from path
	err = mt.ParseName(path)
	if err != nil {
		return nil, err
	}
	mt.Collation = fi._26_CHARSET
	mt.MySQLVersion = fi._33_MYSQL_VERSION
	// get options
	mt.Options = NewOptions(fi)
	mt.Options.Collation = mt.Collation
	// get keys data
	mt.Keys = NewKeysData(data, uint32(fi._06_KEY_INFO_OFFSET), fi.KEYS_DATA_LENGTH)
	// get defaults data
	mt.Defaults = NewDefaultsData(data, fi.DEFAULTS_DATA_OFFSET, uint32(fi._10_RECORD_LENGTH))
	// get extra data
	mt.Extra = NewExtraData(data, fi.EXTRA_DATA_OFFSET, fi._37_EXTRA_INFO_LENGTH)
	// decode keys need extra data
	mt.Keys.Extra = mt.Extra

	// get columns data
	// get metadata
	metadata := column.NewMetadata(data, fi.FORM_INFO_OFFSET+FORM_INFO_LENGTH+uint32(fi.SCREENS_LENGTH), 17*uint32(fi.COLUMN_COUNT))
	// get names data
	namesData := column.NewNamesData(data, metadata.Offset+metadata.Length, uint32(fi.NAMES_LENGTH))
	// get labels data
	labelsData := column.NewLabelsData(data, namesData.Offset+namesData.Length, uint32(fi.LABELS_LENGTH))
	// get comments data
	commentsData := column.NewCommentsData(data, labelsData.Offset+labelsData.Length, uint32(fi.COMMENTS_LENGTH))
	// construct columns data
	mt.Columns = &Columns{
		Count:     fi.COLUMN_COUNT,
		NullCount: fi.NULL_FIELDS,
		Metadata:  metadata,
		Names:     namesData,
		Labels:    labelsData,
		Comments:  commentsData,
		Defaults:  mt.Defaults,
	}
	return mt, nil
}

func (mt *MySQLTable) ParseName(path string) (err error) {
	mt.Name = strings.TrimSuffix(filepath.Base(path), ".frm")
	mt.Name, err = utils.DecodeMySQLFile2Object(mt.Name)
	return err
}

func (mt *MySQLTable) String() string {
	columnKeys := mt.Columns.Combined
	if mt.Keys.Combined != "" {
		columnKeys += ",\n" + mt.Keys.Combined
	}
	parts := []string{
		"",
		fmt.Sprintf("CREATE TABLE `%s` (", mt.Name),
		columnKeys,
		fmt.Sprintf(") %s;", mt.Options.String()),
		"",
	}
	return strings.Join(parts, "\n")
}

func (mt *MySQLTable) StringWithHeader() string {
	parts := []string{
		"",
		"--",
		fmt.Sprintf("-- Table structure for table `%s`", mt.Name),
		fmt.Sprintf("-- Created with MySQL Version %s", mt.MySQLVersion.String()),
		"--",
		mt.String(),
	}
	return strings.Join(parts, "\n")
}

func (mt *MySQLTable) Decode(data []byte) error {
	mt.DecodeOptions()
	err := mt.Columns.Decode(mt)
	if err != nil {
		return err
	}
	mt.Keys.Decode(mt.Columns)
	mt.DecodeTableComment(data)
	return nil
}

func (mt *MySQLTable) DecodeOptions() {
	if mt.Extra.Length <= 2 {
		return
	}
	var skipLength uint32 = 2 // skip null + autopartition flag
	// connection
	connectionLength := binary.LittleEndian.Uint16(mt.Extra.Data)
	engineLengthOffset := 2 + connectionLength
	mt.Options.Connection = string(mt.Extra.Data[2:engineLengthOffset])
	engineOffset := engineLengthOffset + 2
	if mt.Extra.Length < uint32(engineOffset) {
		mt.Extra.CurrentOffset = uint32(engineLengthOffset) + skipLength
		return
	}
	// engine
	engineLength := binary.LittleEndian.Uint16(mt.Extra.Data[engineLengthOffset:])
	partitionLengthOffset := engineOffset + engineLength
	engine := string(mt.Extra.Data[engineOffset:partitionLengthOffset])
	if engine == "" {
		mt.Options.Engine = LegacyDBTypeMap[mt.FileInfo._03_ENGINE]
	} else if engine == "partion" {
		mt.Options.Engine = LegacyDBTypeMap[mt.FileInfo._3D_PARTITION_ENGINE]
	} else {
		mt.Options.Engine = engine
	}
	// partitions
	partitionOffset := partitionLengthOffset + 4
	if mt.Extra.Length <= uint32(partitionOffset) {
		mt.Extra.CurrentOffset = uint32(partitionLengthOffset) + skipLength
		return
	}
	partitionLength := binary.LittleEndian.Uint32(mt.Extra.Data[partitionLengthOffset:])
	mt.Options.Partitions = string(mt.Extra.Data[partitionOffset : uint32(partitionOffset)+partitionLength])
	mt.Extra.CurrentOffset = uint32(partitionOffset) + partitionLength + skipLength
}

func (mt *MySQLTable) DecodeTableComment(data []byte) {
	if mt.FileInfo.TABLE_COMMENT_LENGTH != 0xFF {
		tableCommentOffset := mt.FileInfo.FORM_INFO_OFFSET + 47
		mt.Options.Comment = string(data[tableCommentOffset : tableCommentOffset+uint32(mt.FileInfo.TABLE_COMMENT_LENGTH)])
	} else {
		mt.Options.Comment = mt.Extra.DecodeTableComment()
	}
}
