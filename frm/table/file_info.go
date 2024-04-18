package table

import (
	"encoding/binary"
)

// file info is the header of the file
// ref: https://dbsake.readthedocs.io/en/latest/appendix/frm_format.html#frm-fileinfo-section
type FileInfo struct {
	_00_MAGIC               []byte        // 2 bytes	“Magic” identifier Always the byte sequence fe 01
	_02_VERSION             uint8         // 1 byte		.frm version. This is defined as FRM_VER+3+ test(create_info->varchar) in 5.0+ Where FRM_VER is defined as 6, so the frm version will be either 9 or 10 depending on if the table has varchar columns
	_03_ENGINE              LegacyDBType  // 1 byte		Maps to an enum value from “enum legacy_db_type” in sql/handler.h
	_04_NAMES_LENGTH        uint16        // 2 bytes	“names_length” - always 3 and not used in recent MySQL. MySQL 3.23 set this to 1
	_06_KEY_INFO_OFFSET     uint16        // 2-bytes	IO_SIZE; Always 4096 (0010) // It's key info offset in original dbsake code
	_08_NUM_FORMS           uint16        // 2-bytes	number of “forms” in the .frm Should always be 1, even back to 3.23
	_0A_UNUSED              uint32        // 4-bytes	Not really used except in .frm creation Purpose unclear, i guess for aligning sections in the ancient unireg format
	_0E_TMP_KEY_INFO_LENGTH uint16        // 2-bytes	“tmp_key_info_length”; if equal to 0xffff then the key length is a 4-byte integer at offset 0x002f
	_10_RECORD_LENGTH       uint16        // 2-bytes	“rec_length” - this is the size of the byte string where default values are stored See Default Values
	_12_MAX_ROWS            uint32        // 4-bytes	Table MAX_ROWS=N opton
	_16_MIN_ROWS            uint32        // 4-bytes	Table MIN_ROWS=N option
	_1A_UNUSED              uint8         // 1-byte		Unused - always zero in 3.23 through 5.6
	_1B_USE_LONG_PACK       uint8         // 1-byte		Always 2 - “// Use long pack-fields”
	_1C_KEY_INFO_LENGTH     uint16        // 2-bytes	key_info_length - size in bytes of the keyinfo section
	_1E_HANDLER_OPTION      HandlerOption // 2-bytes	create_info->table_options See HA_OPTION_* values in include/my_base.h
	_20_UNUSED              uint8         // 1-byte		Unused; comment “// No filename anymore”
	_21_MARK_50             uint8         // 1-byte		5 in 5.0+ comment “// Mark for 5.0 frm file”
	_22_AVG_ROW_LENGTH      uint32        // 4-bytes	Table AVG_ROW_LENGTH option
	_26_CHARSET             *Collation    // 1-byte		Table DEFAULT CHARACTER SET option: Character set id maps to an id from INFORMATION_SCHEMA.COLLATIONS and encodes both the character set name and the collation
	_27_UNUSED              uint8         // 1-byte		Unused: In the source code, there is a comment indicating this byte will be used for TRANSACTIONAL and PAGE_CHECKSUM table options in the future
	_28_ROW_TYPE            RowType       // 1-byte		Table ROW_FORMAT option
	_29_UNUSED              uint8         // 1-byte		Unused; formerly Table RAID_TYPE option
	_2A_UNUSED              uint8         // 1-byte		Unused; formerly Table RAID_CHUNKS option
	_2B_UNUSED              uint32        // 4-bytes	Unused; formerly Table RAID_CHUNKSIZE option
	_2F_KEY_INFO_LENGTH     uint32        // 4-bytes	Size in bytes of the keyinfo section where index metadata is defined
	_33_MYSQL_VERSION       *MySQLVersion // 4-bytes	MySQL version encoded as a 4-byte integer in little endian format. This is the value MYSQL_VERSION_ID from include/mysql_version.h in the mysql source tree. Example: ‘xb6xc5x00x00’ 0x0000c5b6 => 50614 => MySQL v5.6.14
	_37_EXTRA_INFO_LENGTH   uint32
	/*
		4-bytes Size in bytes of table “extra info”

			CONNECTION=<string> (FEDERATED tables)
			ENGINE=<string>
			PARTITION BY clause + partitioning flags
			WITH PARSER names (MySQL 5.1+)
			Table COMMENT [5]
				The table comment is stored in one of two places in the .frm file If the comment size in bytes is < 255 this is stored in the forminfo Otherwise it will be estored in the extra info section after the fulltext parser names (if any)
	*/
	_3B_EXTRA_REC_BUF_LENGTH uint16       // 2-bytes	extra_rec_buf_length
	_3D_PARTITION_ENGINE     LegacyDBType // 1-byte	Storage engine if table is partitioned: Numeric id that maps to a enum value from “enum legacy_db_type” in sql/handler.h, similar to legacy_db_type
	_3E_KEY_BLOCK_SIZE       uint16       // 2-bytes	Table KEY_BLOCK_SIZE option

	KEYS_DATA_LENGTH     uint32
	DEFAULTS_DATA_OFFSET uint32
	EXTRA_DATA_OFFSET    uint32

	*FormInfo

	MySQLTable *MySQLTable
}

// NewFileInfo creates a new FileInfo struct
func ReadFileInfo(path string, data []byte) (fi *FileInfo, err error) {
	charset, err := GetCollationByID(int(data[0x26]))
	if err != nil {
		return nil, err
	}
	// fix sized data
	fi = &FileInfo{
		_00_MAGIC:                data[0x00:0x02],
		_02_VERSION:              data[0x02],
		_03_ENGINE:               LegacyDBType(data[0x03]),
		_04_NAMES_LENGTH:         binary.LittleEndian.Uint16(data[0x04:0x06]),
		_06_KEY_INFO_OFFSET:      binary.LittleEndian.Uint16(data[0x06:0x08]),
		_08_NUM_FORMS:            binary.LittleEndian.Uint16(data[0x08:0x0A]),
		_0A_UNUSED:               binary.LittleEndian.Uint32(data[0x0A:0x0E]),
		_0E_TMP_KEY_INFO_LENGTH:  binary.LittleEndian.Uint16(data[0x0E:0x10]),
		_10_RECORD_LENGTH:        binary.LittleEndian.Uint16(data[0x10:0x12]),
		_12_MAX_ROWS:             binary.LittleEndian.Uint32(data[0x12:0x16]),
		_16_MIN_ROWS:             binary.LittleEndian.Uint32(data[0x16:0x1A]),
		_1A_UNUSED:               data[0x1A],
		_1B_USE_LONG_PACK:        data[0x1B],
		_1C_KEY_INFO_LENGTH:      binary.LittleEndian.Uint16(data[0x1C:0x1E]),
		_1E_HANDLER_OPTION:       HandlerOption(binary.LittleEndian.Uint16(data[0x1E:0x20])),
		_20_UNUSED:               data[0x20],
		_21_MARK_50:              data[0x21],
		_22_AVG_ROW_LENGTH:       binary.LittleEndian.Uint32(data[0x22:0x26]),
		_26_CHARSET:              charset,
		_27_UNUSED:               data[0x27],
		_28_ROW_TYPE:             RowType(data[0x28]),
		_29_UNUSED:               data[0x29],
		_2A_UNUSED:               data[0x2A],
		_2B_UNUSED:               binary.LittleEndian.Uint32(data[0x2B:0x2F]),
		_2F_KEY_INFO_LENGTH:      binary.LittleEndian.Uint32(data[0x2F:0x33]),
		_33_MYSQL_VERSION:        NewMySQLVersion(data[0x33:0x37]),
		_37_EXTRA_INFO_LENGTH:    binary.LittleEndian.Uint32(data[0x37:0x3B]),
		_3B_EXTRA_REC_BUF_LENGTH: binary.LittleEndian.Uint16(data[0x3B:0x3D]),
		_3D_PARTITION_ENGINE:     LegacyDBType(data[0x3D]),
		_3E_KEY_BLOCK_SIZE:       binary.LittleEndian.Uint16(data[0x3E:0x40]),
	}
	// get keys data length
	if fi._0E_TMP_KEY_INFO_LENGTH == 0xffff {
		fi.KEYS_DATA_LENGTH = fi._2F_KEY_INFO_LENGTH
	} else {
		fi.KEYS_DATA_LENGTH = uint32(fi._0E_TMP_KEY_INFO_LENGTH)
	}
	// get defaults data offset
	fi.DEFAULTS_DATA_OFFSET = uint32(fi._06_KEY_INFO_OFFSET) + fi.KEYS_DATA_LENGTH
	// get extra data offset
	fi.EXTRA_DATA_OFFSET = fi.DEFAULTS_DATA_OFFSET + uint32(fi._10_RECORD_LENGTH)
	// read form info
	fi.FormInfo, err = ReadFormInfo(data, fi)
	if err != nil {
		return nil, err
	}
	fi.MySQLTable = NewMySQLTable(path, data, fi)
	return fi, nil
}
