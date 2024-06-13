package table

import (
	"fmt"
)

const (
	// FILE_INFO_LENGTH is the size of the file header
	FILE_INFO_LENGTH = 64

	// FORM_INFO_LENGTH is the length of the form info
	FORM_INFO_LENGTH = 288

	// MAX_DATE_WIDTH is the maximum width of a date
	MAX_DATE_WIDTH = 10

	// MAX_TIME_WIDTH is the maximum width of a time
	MAX_TIME_WIDTH = 10

	// MAX_TIME_FULL_WIDTH is the maximum width of a time with fractional seconds
	MAX_TIME_FULL_WIDTH = 23

	// MAX_DATETIME_WIDTH is the maximum width of a datetime
	MAX_DATETIME_WIDTH = 19
)

// LegacyDBType represents the legacy database types
type LegacyDBType uint8

const (
	LDBT_UNKNOWN LegacyDBType = iota
	LDBT_DIAB_ISAM
	LDBT_HASH
	LDBT_MISAM
	LDBT_PISAM
	LDBT_RMS_ISAM
	LDBT_HEAP
	LDBT_ISAM
	LDBT_MRG_ISAM
	LDBT_MyISAM
	LDBT_MRG_MYISAM
	LDBT_BERKELEYDB
	LDBT_InnoDB
	LDBT_GEMINI
	LDBT_NDBCLUSTER
	LDBT_EXAMPLE_DB
	LDBT_ARCHIVE_DB
	LDBT_CSV
	LDBT_FEDERATED
	LDBT_BLACKHOLE
	LDBT_PARTITION_DB
	LDBT_BINLOG
	LDBT_SOLID
	LDBT_PBXT
	LDBT_TABLE_FUNCTION
	LDBT_MEMCACHE
	LDBT_FALCON
	LDBT_MARIA
	LDBT_PERFORMANCE_SCHEMA
	LDBT_FIRST_DYNAMIC = 42
	LDBT_DEFAULT       = 127
)

var LegacyDBTypeMap = map[LegacyDBType]string{
	LDBT_UNKNOWN:            "UNKNOWN",
	LDBT_DIAB_ISAM:          "DIAB_ISAM",
	LDBT_HASH:               "HASH",
	LDBT_MISAM:              "MISAM",
	LDBT_PISAM:              "PISAM",
	LDBT_RMS_ISAM:           "RMS_ISAM",
	LDBT_HEAP:               "HEAP",
	LDBT_ISAM:               "ISAM",
	LDBT_MRG_ISAM:           "MRG_ISAM",
	LDBT_MyISAM:             "MyISAM",
	LDBT_MRG_MYISAM:         "MRG_MYISAM",
	LDBT_BERKELEYDB:         "BERKELEYDB",
	LDBT_InnoDB:             "InnoDB",
	LDBT_GEMINI:             "GEMINI",
	LDBT_NDBCLUSTER:         "NDBCLUSTER",
	LDBT_EXAMPLE_DB:         "EXAMPLE_DB",
	LDBT_ARCHIVE_DB:         "ARCHIVE_DB",
	LDBT_CSV:                "CSV",
	LDBT_FEDERATED:          "FEDERATED",
	LDBT_BLACKHOLE:          "BLACKHOLE",
	LDBT_PARTITION_DB:       "PARTITION_DB",
	LDBT_BINLOG:             "BINLOG",
	LDBT_SOLID:              "SOLID",
	LDBT_PBXT:               "PBXT",
	LDBT_TABLE_FUNCTION:     "TABLE_FUNCTION",
	LDBT_MEMCACHE:           "MEMCACHE",
	LDBT_FALCON:             "FALCON",
	LDBT_MARIA:              "MARIA",
	LDBT_PERFORMANCE_SCHEMA: "PERFORMANCE_SCHEMA",
	LDBT_FIRST_DYNAMIC:      "FIRST_DYNAMIC",
	LDBT_DEFAULT:            "DEFAULT",
}

// FieldFlag represents the flags for a field
type FieldFlag int

const (
	FF_DECIMAL           FieldFlag = 1
	FF_BINARY            FieldFlag = 1
	FF_NUMBER            FieldFlag = 2
	FF_ZEROFILL          FieldFlag = 4
	FF_PACK              FieldFlag = 120
	FF_INTERVAL          FieldFlag = 256
	FF_BITFIELD          FieldFlag = 512
	FF_BLOB              FieldFlag = 1024
	FF_GEOM              FieldFlag = 2048
	FF_JSON              FieldFlag = 4096
	FF_TREAT_BIT_AS_CHAR FieldFlag = 4096
	FF_NO_DEFAULT        FieldFlag = 16384
	FF_MAYBE_NULL        FieldFlag = 32768
	FF_HEX_ESCAPE        FieldFlag = 0x10000
	FF_PACK_SHIFT        FieldFlag = 3
	FF_DEC_SHIFT         FieldFlag = 8
	FF_MAX_DEC           FieldFlag = 31
	FF_NUM_SCREEN_TYPE   FieldFlag = 0x7F01
	FF_ALFA_SCREEN_TYPE  FieldFlag = 0x7800
)

func (fs FieldFlag) HasFlag(f FieldFlag) bool {
	return fs&f != 0
}

// Utype represents the unireg types
type Utype uint8

const (
	UT_NONE Utype = iota
	UT_DATE
	UT_SHIELD
	UT_NOEMPTY
	UT_CASEUP
	UT_PNR
	UT_BGNR
	UT_PGNR
	UT_YES
	UT_NO
	UT_REL
	UT_CHECK
	UT_EMPTY
	UT_UNKNOWN_FIELD
	UT_CASEDN
	UT_NEXT_NUMBER
	UT_INTERVAL_FIELD
	UT_BIT_FIELD
	UT_TIMESTAMP_OLD_FIELD
	UT_CAPITALIZE
	UT_BLOB_FIELD
	UT_TIMESTAMP_DN_FIELD
	UT_TIMESTAMP_UN_FIELD
	UT_TIMESTAMP_DNUN_FIELD
)

// MySQLType represents the MySQL field types
type MySQLType uint8

const (
	MT_DECIMAL MySQLType = iota
	MT_TINY
	MT_SHORT
	MT_LONG
	MT_FLOAT
	MT_DOUBLE
	MT_NULL
	MT_TIMESTAMP
	MT_LONGLONG
	MT_INT24
	MT_DATE
	MT_TIME
	MT_DATETIME
	MT_YEAR
	MT_NEWDATE
	MT_VARCHAR
	MT_BIT
	MT_TIMESTAMP2
	MT_DATETIME2
	MT_TIME2
	MT_JSON = iota + 225
	MT_NEWDECIMAL
	MT_ENUM
	MT_SET
	MT_TINY_BLOB
	MT_MEDIUM_BLOB
	MT_LONG_BLOB
	MT_BLOB
	MT_VAR_STRING
	MT_STRING
	MT_GEOMETRY
)

type KeyPrefix uint8

const (
	KP_NONE KeyPrefix = iota
	KP_MAYBE
	KP_ALWAYS
)

var MySQLTypeMap = map[MySQLType]struct {
	Prefix      string
	IsKeyPrefix KeyPrefix
}{
	MT_DECIMAL:     {"decimal", KP_NONE},
	MT_TINY:        {"tinyint", KP_NONE},
	MT_SHORT:       {"smallint", KP_NONE},
	MT_LONG:        {"int", KP_NONE},
	MT_FLOAT:       {"float", KP_NONE},
	MT_DOUBLE:      {"double", KP_NONE},
	MT_NULL:        {"null", KP_NONE},
	MT_TIMESTAMP:   {"timestamp", KP_NONE},
	MT_LONGLONG:    {"bigint", KP_NONE},
	MT_INT24:       {"mediumint", KP_NONE},
	MT_DATE:        {"date", KP_NONE},
	MT_TIME:        {"time", KP_NONE},
	MT_DATETIME:    {"datetime", KP_NONE},
	MT_YEAR:        {"year", KP_NONE},
	MT_NEWDATE:     {"date", KP_NONE},
	MT_VARCHAR:     {"var", KP_MAYBE},
	MT_BIT:         {"bit", KP_NONE},
	MT_TIMESTAMP2:  {"timestamp", KP_NONE},
	MT_DATETIME2:   {"datetime", KP_NONE},
	MT_TIME2:       {"time", KP_NONE},
	MT_JSON:        {"json", KP_NONE},
	MT_NEWDECIMAL:  {"decimal", KP_NONE},
	MT_ENUM:        {"enum", KP_NONE},
	MT_SET:         {"set", KP_NONE},
	MT_TINY_BLOB:   {"tiny", KP_ALWAYS},
	MT_MEDIUM_BLOB: {"medium", KP_ALWAYS},
	MT_LONG_BLOB:   {"long", KP_ALWAYS},
	MT_BLOB:        {"", KP_ALWAYS},
	MT_VAR_STRING:  {"var", KP_MAYBE},
	MT_STRING:      {"", KP_MAYBE},
	MT_GEOMETRY:    {"geometry", KP_ALWAYS},
}

func (mt MySQLType) Name() (string, error) {
	prefix, ok := MySQLTypeMap[mt]
	if !ok {
		return "", fmt.Errorf("unknown MySQLType: %d", mt)
	}
	return prefix.Prefix, nil
}

func (mt MySQLType) KeyPrefix() (KeyPrefix, error) {
	prefix, ok := MySQLTypeMap[mt]
	if !ok {
		return 0, fmt.Errorf("unknown MySQLType: %d", mt)
	}
	return prefix.IsKeyPrefix, nil
}

// GeometryType represents the geometry types
type GeometryType uint8

const (
	GT_GEOMETRY GeometryType = iota
	GT_POINT
	GT_LINESTRING
	GT_POLYGON
	GT_MULTIPOINT
	GT_MULTILINESTRING
	GT_MULTIPOLYGON
	GT_GEOMETRYCOLLECTION
)

var GeometryTypeMap = map[GeometryType]string{
	GT_GEOMETRY:           "geometry",
	GT_POINT:              "point",
	GT_LINESTRING:         "linestring",
	GT_POLYGON:            "polygon",
	GT_MULTIPOINT:         "multipoint",
	GT_MULTILINESTRING:    "multilinestring",
	GT_MULTIPOLYGON:       "multipolygon",
	GT_GEOMETRYCOLLECTION: "geometrycollection",
}

func (gt GeometryType) Name() (string, error) {
	name, ok := GeometryTypeMap[gt]
	if !ok {
		return "", fmt.Errorf("unknown GeometryType: %d", gt)
	}
	return name, nil
}

// HandlerOption represents the HA_OPTION flags
type HandlerOption uint16

const (
	HO_PACK_RECORD          HandlerOption = 1
	HO_PACK_KEYS            HandlerOption = 2
	HO_COMPRESS_RECORD      HandlerOption = 4
	HO_LONG_BLOB_PTR        HandlerOption = 8 // new ISAM format
	HO_TMP_TABLE            HandlerOption = 16
	HO_CHECKSUM             HandlerOption = 32
	HO_DELAY_KEY_WRITE      HandlerOption = 64
	HO_NO_PACK_KEYS         HandlerOption = 128 // Reserved for MySQL
	HO_CREATE_FROM_ENGINE   HandlerOption = 256
	HO_RELIES_ON_SQL_LAYER  HandlerOption = 512
	HO_NULL_FIELDS          HandlerOption = 1024
	HO_PAGE_CHECKSUM        HandlerOption = 2048
	HO_STATS_PERSISTENT     HandlerOption = 4096
	HO_NO_STATS_PERSISTENT  HandlerOption = 8192
	HO_TEMP_COMPRESS_RECORD HandlerOption = 16384 // set by isamchk
	HO_READ_ONLY_DATA       HandlerOption = 32768 // Set by isamchk
)

func (hos HandlerOption) HasOption(ho HandlerOption) bool {
	return hos&ho != 0
}

// RowType represents the row types
type RowType uint8

const (
	RT_DEFAULT RowType = iota
	RT_FIXED
	RT_DYNAMIC
	RT_COMPRESSED
	RT_REDUNDANT
	RT_COMPACT
	RT_UNKNOWN_6
	RT_TOKUDB_UNCOMPRESSED
	RT_TOKUDB_ZLIB
	RT_TOKUDB_SNAPPY
	RT_TOKUDB_QUICKLZ
	RT_TOKUDB_LZMA
	RT_TOKUDB_FAST
	RT_TOKUDB_SMALL
	RT_TOKUDB_DEFAULT
	RT_UNKNOWN_15
	RT_UNKNOWN_16
	RT_UNKNOWN_17
	RT_UNKNOWN_18
)

func (h RowType) String() string {
	switch h {
	case RT_TOKUDB_DEFAULT:
		return "TOKUDB_ZLIB"
	case RT_TOKUDB_FAST:
		return "TOKUDB_QUICKLZ"
	case RT_TOKUDB_SMALL:
		return "TOKUDB_LZMA"
	default:
		return [...]string{"", "FIXED", "DYNAMIC", "COMPRESSED", "REDUNDANT", "COMPACT", "?", "?", "?", "?", "?"}[h]
	}
}

// constants for key

type HaKeyFlag uint16

const (
	HA_NOSAME          HaKeyFlag = 1 // Set if not dupplicated records
	HA_PACK_KEY        HaKeyFlag = 2 // Pack string key to previous key
	HA_AUTO_KEY        HaKeyFlag = 16
	HA_BINARY_PACK_KEY HaKeyFlag = 32   // Packing of all keys to prev key
	HA_FULLTEXT        HaKeyFlag = 128  // For full-text search
	HA_UNIQUE_CHECK    HaKeyFlag = 256  // Check the key for uniqueness
	HA_SPATIAL         HaKeyFlag = 1024 // For spatial search
	HA_NULL_ARE_EQUAL  HaKeyFlag = 2048 // NULL in key are cmp as equal
	HA_USES_COMMENT    HaKeyFlag = 4096
	HA_USES_PARSER     HaKeyFlag = 16384 // Fulltext index uses [pre]parser
	HA_GENERATED_KEY   HaKeyFlag = 8192  // Automaticly generated key
)

func (kfs HaKeyFlag) HasFlag(f HaKeyFlag) bool {
	return kfs&f != 0
}

type HaKeyAlgo uint8

const (
	HA_KEY_ALG_UNDEF    HaKeyAlgo = iota // Not specified (old file)
	HA_KEY_ALG_BTREE                     // B-tree, default one
	HA_KEY_ALG_RTREE                     // R-tree, for spatial searches
	HA_KEY_ALG_HASH                      // HASH keys (HEAP tables)
	HA_KEY_ALG_FULLTEXT                  // FULLTEXT (MyISAM tables)
)

var KeyAlgoMap = map[HaKeyAlgo]string{
	HA_KEY_ALG_UNDEF:    "",
	HA_KEY_ALG_BTREE:    "BTREE",
	HA_KEY_ALG_RTREE:    "RTREE",
	HA_KEY_ALG_HASH:     "HASH",
	HA_KEY_ALG_FULLTEXT: "FULLTEXT",
}

func (ka HaKeyAlgo) Name() string {
	return KeyAlgoMap[ka]
}
