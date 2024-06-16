package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/zing22845/go-frm-parser/frm/table/column"
	"github.com/zing22845/go-frm-parser/frm/utils"
)

type Column struct {
	Name           string
	Number         int
	TypeCode       MySQLType
	TypeName       string
	Length         uint16
	Attributes     []string
	LabelStrs      []string
	Default        string
	Comment        string
	Collation      *Collation
	TableCollation *Collation
	SubTypeCode    GeometryType
	Flags          FieldFlag
	Utype          Utype
	NullBitMap     []byte
	NullBit        int
	Metadata       *column.Metadata
	Defaults       *Defaults
	Labels         *column.Labels
	Comments       *column.Comments
	Scale          FieldFlag
}

func (c *Column) String() string {
	components := []string{
		fmt.Sprintf("`%s`", strings.ReplaceAll(c.Name, "`", "``")),
		c.TypeName,
	}
	if c.Default != "" {
		components = append(components, fmt.Sprintf("DEFAULT %s", c.Default))
	}
	if c.Comment != "" {
		components = append(components, fmt.Sprintf("COMMENT '%s'", strings.ReplaceAll(c.Comment, "'", "\\'")))
	}
	return strings.Join(components, " ")
}

func (c *Column) Decode() (err error) {
	c.Length = binary.LittleEndian.Uint16(c.Metadata.Data[c.Metadata.CurrentOffset+3 : c.Metadata.CurrentOffset+5])
	// decode flags
	c.Flags = FieldFlag(binary.LittleEndian.Uint16(c.Metadata.Data[c.Metadata.CurrentOffset+8 : c.Metadata.CurrentOffset+10]))
	// decode unireg_check
	c.Utype = Utype(c.Metadata.Data[c.Metadata.CurrentOffset+10])
	// deocde type code
	c.TypeCode = MySQLType(c.Metadata.Data[c.Metadata.CurrentOffset+13])
	// get LabelBytes for ENUM or SET columns
	var labelBytes [][]byte
	if c.TypeCode == MT_ENUM || c.TypeCode == MT_SET {
		labelID := int(c.Metadata.Data[c.Metadata.CurrentOffset+12]) - 1
		if labelID >= 0 {
			labelBytes = c.Labels.Items[labelID]
		}
	}
	c.Defaults.CurrentOffset = uint32(utils.Uint24LE(c.Metadata.Data[c.Metadata.CurrentOffset+5:c.Metadata.CurrentOffset+8])) - 1
	// decode comment length
	commentLength := binary.LittleEndian.Uint16(c.Metadata.Data[c.Metadata.CurrentOffset+15 : c.Metadata.CurrentOffset+17])

	// decode collation id for column type
	var collationID int
	if c.TypeCode != MT_GEOMETRY {
		collationID = (int(c.Metadata.Data[c.Metadata.CurrentOffset+11]) << 8) + int(c.Metadata.Data[c.Metadata.CurrentOffset+14])
		c.SubTypeCode = 0
	} else {
		collationID = 63 // binary
		c.SubTypeCode = GeometryType(c.Metadata.Data[c.Metadata.CurrentOffset+14])
	}
	c.Metadata.CurrentOffset += 17 // move to next column for metadata decoding
	// deocde charset collation
	c.Collation, err = GetCollationByID(collationID)
	if err != nil {
		return err
	}
	// decode labels name by charset to utf8
	if labelBytes != nil {
		c.LabelStrs = make([]string, len(labelBytes))
		for i, lb := range labelBytes {
			c.LabelStrs[i], err = utils.UTF8Decoder(lb, c.Collation.CharsetName)
			if err != nil {
				c.LabelStrs[i] = string(lb)
			}
		}
	}
	// decode type name and defaults
	err = c.DecodeTypes()
	if err != nil {
		return err
	}
	// decode comment
	c.Comment, err = c.Comments.Decode(
		uint32(commentLength), c.Collation.CharsetName)
	if err != nil {
		return err
	}
	return nil
}

func (c *Column) DecodeTypes() (err error) {
	// Utype.NEXT_NUMBER (AUTO_INCREMENT) columns will never have a default
	// blob fields also never have a default in any current MySQL version but
	// some mysql forks don't set the NO_DEFAULT field flag, so default
	// processing is special cased here to handle these cases
	// get default null
	// suppress default for blob types
	hasDefault := c.hasDefaults()
	c.Scale = (c.Flags >> FF_DEC_SHIFT) & FF_MAX_DEC
	// init type name prefix
	c.TypeName, err = c.TypeCode.Name()
	if err != nil {
		return err
	}
	switch c.TypeCode {
	case MT_DECIMAL, MT_NEWDECIMAL:
		c.decodeTypeDecimal(hasDefault)
	case MT_TINY, MT_SHORT, MT_INT24, MT_LONG, MT_LONGLONG:
		c.decodeTypeInteger(hasDefault)
	case MT_FLOAT, MT_DOUBLE:
		c.decodeTypeReal(hasDefault)
	case MT_STRING, MT_VAR_STRING, MT_VARCHAR:
		err = c.decodeTypeChars(hasDefault)
		if err != nil {
			return err
		}
	case MT_ENUM, MT_SET:
		c.decodeTypeEnumSet(hasDefault)
	case MT_TINY_BLOB, MT_MEDIUM_BLOB, MT_LONG_BLOB, MT_BLOB:
		err = c.decodeTypeBlob(hasDefault)
		if err != nil {
			return err
		}
	case MT_JSON:
		err = c.decodeTypeJson(hasDefault)
		if err != nil {
			return err
		}
	case MT_BIT:
		c.decodeTypeBit(hasDefault)
	case MT_TIME, MT_TIME2:
		err = c.decodeTypeTime(hasDefault)
		if err != nil {
			return err
		}
	case MT_TIMESTAMP, MT_TIMESTAMP2, MT_DATETIME, MT_DATETIME2:
		err = c.decodeTypeDatetime(hasDefault)
		if err != nil {
			return err
		}
	case MT_YEAR:
		c.decodeTypeYear(hasDefault)
	case MT_DATE, MT_NEWDATE:
		err = c.decodeTypeDate(hasDefault)
		if err != nil {
			return err
		}
	case MT_GEOMETRY:
		err = c.decodeTypeGeometry(hasDefault)
		if err != nil {
			return err
		}
	}
	// add additional type information
	if !c.Flags.HasFlag(FF_MAYBE_NULL) {
		c.TypeName += " NOT NULL"
	}
	if c.Utype == UT_NEXT_NUMBER {
		c.TypeName += " AUTO_INCREMENT"
	}
	return nil
}

func (c *Column) hasDefaults() bool {
	isAutoIncrement := (c.Utype == UT_NEXT_NUMBER)
	if c.Flags.HasFlag(FF_NO_DEFAULT) || isAutoIncrement {
		return false
	}
	if c.Flags.HasFlag(FF_MAYBE_NULL) {
		offset := c.NullBit / 8
		nullByte := c.NullBitMap[offset]
		nullBit := c.NullBit % 8
		c.NullBit++
		if nullByte&(1<<(nullBit)) != 0 && c.Utype != UT_BLOB_FIELD {
			c.Default = "NULL"
			return false
		}
	}

	return c.Utype != UT_BLOB_FIELD
}

func (c *Column) decodeTypeDecimal(hasDefaults bool) {
	precision := c.Length
	if c.Scale != 0 {
		precision -= 1
	}
	if precision != 0 {
		precision -= 1
	}
	c.TypeName += fmt.Sprintf("(%d,%d)", precision, c.Scale)
	if hasDefaults {
		c.decodeDecimalDefault(precision)

	}
}

func (c *Column) decodeDecimalDefault(precision uint16) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	if c.TypeCode == MT_DECIMAL {
		data = data[:c.Length]
		c.Default = fmt.Sprintf("'%s'", string(data))
		return
	}
	// decode default for new decimal
	intLength, fracLength := utils.CalculateDecimalLengths(int(precision), int(c.Scale))
	data = data[:intLength+fracLength]
	first := data[0]
	sign := ""
	if first&0x80 == 0 {
		sign = "-"
	}
	data[0] ^= 0x80
	data = append([]byte{data[0]}, data[1:]...)
	c.Default = "'" + sign

	// decode integer part
	if intLength > 0 {
		integerPart := c.decodeDecimal(data[:intLength], len(sign) != 0)
		// remove insignificant zeros but ensure we have
		integerPart = strings.TrimLeft(integerPart, "0")
		// at least one digit
		if integerPart == "" {
			integerPart = "0"
		}
		c.Default += integerPart
	} else {
		c.Default += "0"
	}
	// decode fractional part
	if fracLength > 0 {
		fracPart := c.decodeDecimal(data[len(data)-fracLength:], len(sign) != 0)
		fracPart = utils.Zfill(fracPart, int(c.Scale))
		c.Default += "." + fracPart
	}
	c.Default += "'"
}

func (c *Column) decodeDecimal(data []byte, invert bool) string {
	/*
		Decode the decimal digits from a set of bytes

		This does not zero pad fractional digits - so these
		may need to be zerofilled or otherwise shifted. Only
		the raw decimal number string represented by the bytes
		will be returned without leading zeros.

		This is intended to decode MySQL's scheme of encoding
		up to 9 decimal digits into a 4 byte word for its
		fixed precision DECIMAL type.

		return string of decimal numbers

		Examples:
		     b'\x01' -> '1'
		     b'\x63' -> '99'
		     b'\x3b\x9a\xc9\xff' -> '999999999'
	*/
	modcheck := len(data) % 4
	if modcheck != 0 {
		pad := 4 - modcheck
		var padChar byte
		if invert {
			padChar = 0xFF
		} else {
			padChar = 0x00
		}
		whole := data[:len(data)-modcheck]
		frac := append(bytes.Repeat([]byte{padChar}, pad), data[len(data)-modcheck:]...)
		data = append(whole, frac...)
	}

	var groups []int32
	for i := 0; i < len(data); i += 4 {
		num := int32(binary.BigEndian.Uint32(data[i : i+4]))
		if invert {
			num = ^num
		}
		groups = append(groups, num)
	}

	parts := make([]string, len(groups))
	for i, g := range groups {
		parts[i] = strconv.Itoa(int(g))
	}

	return strings.Join(parts, "")
}

func (c *Column) decodeTypeInteger(hasDefault bool) {
	if c.Length > 0 {
		c.TypeName += fmt.Sprintf("(%d)", c.Length)
	}
	isSigned := c.Flags.HasFlag(FF_DECIMAL)
	if !isSigned {
		c.TypeName += " unsigned"
	}
	if c.Flags.HasFlag(FF_ZEROFILL) {
		c.TypeName += " zerofill"
	}
	if hasDefault {
		c.decodeNumberDefault(isSigned)
	}
}

func (c *Column) decodeNumberDefault(isSigned bool) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch c.TypeCode {
	case MT_TINY:
		if isSigned {
			c.Default = fmt.Sprintf("'%d'", int8(data[0]))
		} else {
			c.Default = fmt.Sprintf("'%d'", uint8(data[0]))
		}
	case MT_SHORT:
		if isSigned {
			c.Default = fmt.Sprintf("'%d'", int16(binary.LittleEndian.Uint16(data)))
		} else {
			c.Default = fmt.Sprintf("'%d'", binary.LittleEndian.Uint16(data))
		}
	case MT_INT24:
		if isSigned {
			c.Default = fmt.Sprintf("'%d'", int32(utils.Uint24LE(data)))
		} else {
			c.Default = fmt.Sprintf("'%d'", utils.Uint24LE(data))
		}
	case MT_LONG:
		if isSigned {
			c.Default = fmt.Sprintf("'%d'", int32(binary.LittleEndian.Uint32(data)))
		} else {
			c.Default = fmt.Sprintf("'%d'", binary.LittleEndian.Uint32(data))
		}
	case MT_LONGLONG:
		if isSigned {
			c.Default = fmt.Sprintf("'%d'", int64(binary.LittleEndian.Uint64(data)))
		} else {
			c.Default = fmt.Sprintf("'%d'", binary.LittleEndian.Uint64(data))
		}
	}
}

func (c *Column) decodeTypeReal(hasDefaults bool) {
	var precision uint16
	if c.Scale == 0 {
		precision = c.Length
		// if scale is way out of range, this probably means
		// we shouldn't format the <type>(M,D) syntax
		if c.Scale != FF_MAX_DEC {
			c.TypeName += fmt.Sprintf("(%d,%d)", precision, c.Scale)
		}
	}
	isSigned := c.Flags.HasFlag(FF_DECIMAL)
	if !isSigned {
		c.TypeName += " unsigned"
	}
	if c.Flags.HasFlag(FF_ZEROFILL) {
		c.TypeName += " zerofill"
	}
	if hasDefaults {
		c.decodeRealDefault(precision)
	}
}

func (c *Column) decodeRealDefault(precision uint16) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch c.TypeCode {
	case MT_FLOAT:
		value := math.Float32frombits(binary.LittleEndian.Uint32(data))
		if c.Scale >= FF_MAX_DEC {
			c.Default = fmt.Sprintf("'%.6g'", value)
		} else {
			maxScale := precision
			if precision > 16 {
				maxScale = 16
			}
			base := fmt.Sprintf("'%.*g'", maxScale, value)
			parts := strings.Split(base, ".")
			intPart := parts[0]
			decPart := ""
			if len(parts) > 1 {
				decPart = parts[1]
			}
			if len(decPart) < int(c.Scale) {
				decPart = utils.Zfill(decPart, int(c.Scale))
			}
			c.Default = fmt.Sprintf("'%s.%s'", intPart, decPart)
		}
	case MT_DOUBLE:
		value := math.Float64frombits(binary.LittleEndian.Uint64(data))
		if c.Scale >= FF_MAX_DEC {
			c.Default = fmt.Sprintf("'%.6g'", value)
		} else {
			maxScale := precision
			if precision > 16 {
				maxScale = 16
			}
			base := fmt.Sprintf("'%.*g'", maxScale, value)
			parts := strings.Split(base, ".")
			intPart := parts[0]
			decPart := ""
			if len(parts) > 1 {
				decPart = parts[1]
			}
			if len(decPart) < int(c.Scale) {
				decPart = utils.Zfill(decPart, int(c.Scale))
			}
			c.Default = fmt.Sprintf("'%s.%s'", intPart, decPart)
		}
	}
}

func (c *Column) formatCharset() {
	if c.Collation != c.TableCollation && c.Collation.CharsetName != charset.CharsetBin {
		c.TypeName += " CHARACTER SET " + c.Collation.CharsetName
	}
	if !c.Collation.IsDefault {
		c.TypeName += " COLLATE " + c.Collation.Name
	}
}

func (c *Column) decodeTypeChars(hasDefault bool) error {
	if c.Collation.CharsetName == charset.CharsetBin {
		c.TypeName += "binary"
	} else {
		c.TypeName += "char"
	}
	c.TypeName += fmt.Sprintf("(%d)", c.Length/uint16(c.Collation.Maxlen))
	c.formatCharset()
	if hasDefault {
		err := c.decodeCharsDefault()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Column) decodeCharsDefault() (err error) {
	var nBytes uint16
	var dataOffset uint32
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch c.TypeCode {
	case MT_STRING:
		// Unpack a CHAR(N) fixed length string
		// Trailing spaces are always stripped for CHAR fields
		nBytes = c.Length
	case MT_VAR_STRING:
		// Unpack a MySQL 4.1 VARCHAR(N) default value
		// This is the 4.1 varchar type, but with trailing whitespace
		// that pads up to VARCHAR(N) bytes
		// e.g. VARCHAR(5) default 'a' -> 'a ' in 4.1
		// so we use the same logic as decode MT_VARCHAR, but then
		// strip the trailing whitespace
		nBytes = c.Length
	case MT_VARCHAR:
		if c.Length < 256 {
			nBytes = uint16(data[0])
			dataOffset = 1
		} else {
			nBytes = binary.LittleEndian.Uint16(data)
			dataOffset = 2
		}
	}
	// parse data
	data = data[dataOffset : dataOffset+uint32(nBytes)]
	if c.Collation.CharsetName == charset.CharsetBin && c.TypeCode != MT_VAR_STRING {
		data = bytes.Replace(data, []byte{0x00}, []byte{'\\', '0'}, -1)
		c.Default = string(data)
	} else {
		c.Default, err = utils.UTF8Decoder(data, c.Collation.CharsetName)
		if err != nil {
			c.Default = string(data)
		}
	}
	c.Default = fmt.Sprintf("'%s'", strings.TrimRight(c.Default, " "))
	return nil
}

func (c *Column) decodeTypeEnumSet(hasDefault bool) {
	c.TypeName += fmt.Sprintf("('%s')", strings.Join(c.LabelStrs, "','"))
	c.formatCharset()
	if hasDefault {
		c.decodeEnumSetDefault()
	}
}

func (c *Column) decodeEnumSetDefault() (err error) {
	switch c.TypeCode {
	case MT_ENUM:
		err = c.decodeEnumDefault()
		if err != nil {
			return err
		}
	case MT_SET:
		err = c.decodeSetDefault()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Column) decodeEnumDefault() error {
	var offset uint16
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	if len(c.LabelStrs) < 256 {
		offset = uint16(data[0]) - 1
	} else {
		offset = binary.LittleEndian.Uint16(data) - 1
	}
	if int(offset) >= len(c.LabelStrs) {
		return fmt.Errorf("enum default offset %d out of range %d", offset, len(c.LabelStrs))
	}
	c.Default = fmt.Sprintf("'%s'", c.LabelStrs[offset])
	return nil
}

func (c *Column) decodeSetDefault() error {
	eltCount := len(c.LabelStrs)
	nBytes := (eltCount + 7) / 8
	if nBytes > 4 {
		nBytes = 8
	}
	var value uint64
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch nBytes {
	case 1:
		value = uint64(data[0])
	case 2:
		value = uint64(binary.LittleEndian.Uint16(data))
	case 3:
		value = uint64(utils.Uint24LE(data)) // Assume little-endian
	case 4:
		value = uint64(binary.LittleEndian.Uint32(data))
	case 8:
		value = binary.LittleEndian.Uint64(data)
	default:
		return fmt.Errorf("sets cannot have more than 64 elements")
	}
	var result []string
	for bit, name := range c.LabelStrs {
		if value&(1<<uint(bit)) != 0 {
			result = append(result, name)
		}
	}
	c.Default = fmt.Sprintf("'%s'", strings.Join(result, ","))
	return nil
}

func (c *Column) decodeTypeJson(hasDefault bool) error {
	if hasDefault {
		return fmt.Errorf("not implemented default for json type")
	}
	c.Default = "NULL"
	return nil
}

func (c *Column) decodeTypeBlob(hasDefault bool) error {
	if c.Collation.CharsetName == charset.CharsetBin {
		c.TypeName += "blob"
	} else {
		c.TypeName += "text"
	}
	c.formatCharset()
	if hasDefault {
		return fmt.Errorf("not implemented default for blob type")
	}
	return nil
}

func (c *Column) decodeTypeBit(hasDefault bool) {
	c.TypeName += fmt.Sprintf("(%d)", c.Length)
	if hasDefault {
		c.decodeBitDefault()
	}
}

func (c *Column) decodeBitDefault() {
	nbytes := int(c.Length+7) / 8
	pad := bytes.Repeat([]byte{0x00}, 8-nbytes)

	data := c.Defaults.Data[c.Defaults.CurrentOffset : c.Defaults.CurrentOffset+uint32(nbytes)]
	data = append(pad, data...)
	value := binary.BigEndian.Uint64(data)
	c.Default = fmt.Sprintf("b'%b'", value)
}

func (c *Column) decodeTypeTime(hasDefault bool) (err error) {
	scale := int32(c.Length) - MAX_TIME_WIDTH - 1
	if scale > 0 {
		c.TypeName += fmt.Sprintf("(%d)", scale)
	}
	if hasDefault {
		err = c.decodeTimeDefault(scale)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Column) decodeTimeDefault(scale int32) (err error) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch c.TypeCode {
	case MT_TIME:
		if scale > 0 {
			err = c.decodeTimeHiresDefault(scale)
			if err != nil {
				return err
			}
		} else {
			value := utils.Uint24LE(data)
			hour := value / 10000
			minute := (value / 100) % 100
			second := value % 100
			c.Default = fmt.Sprintf("'%02d:%02d:%02d'", hour, minute, second)
		}
	case MT_TIME2:
		c.decodeTime2Default(scale)
	}
	return nil
}

func (c *Column) decodeTime2Default(scale int32) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	// decde HMS part
	hmsBytes := data[:3]
	isNeg := (hmsBytes[0] & 0x80) == 0
	hmsBytes[0] -= 0x80
	hmsBytes = append([]byte{0x00}, hmsBytes...)
	value := int32(binary.BigEndian.Uint32(hmsBytes))
	if isNeg {
		value = ^value
	}
	hour := int((value >> 12) & 0x3FF)
	minute := int((value >> 6) & 0x3F)
	second := int(value & 0x3F)
	result := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)

	// decode fractional part
	if scale > 0 {
		nBytes := utils.DigitsToBytes[scale]
		fracBytes := data[3 : 3+nBytes]
		if len(fracBytes)%4 != 0 {
			pad := 4 - len(fracBytes)%4
			padChar := byte(0x00)
			if isNeg {
				padChar = 0xFF
			}
			fracBytes = append(bytes.Repeat([]byte{padChar}, pad), fracBytes...)
		}
		fracPart := int32(binary.BigEndian.Uint32(fracBytes))
		if fracPart < 0 {
			fracPart = -fracPart
		}
		fracStr := utils.Zfill(strconv.Itoa(int(fracPart)), int(scale))
		if len(fracStr) > int(scale) {
			fracStr = fracStr[:scale]
		}
		result += "." + fracStr
	}
	if isNeg {
		result = "-" + result
	}
	c.Default = fmt.Sprintf("'%s'", result)
}

// Date/Time types
var TIME_HIRES_BYTES = []byte{3, 4, 4, 5, 5, 5, 6}

const (
	TIME_MAX_HOUR           = 838
	TIME_MAX_MINUTE         = 59
	TIME_MAX_SECOND         = 59
	TIME_MAX_SECOND_PART    = 999999
	TIME_SECOND_PART_FACTOR = (TIME_MAX_SECOND_PART + 1)
	TIME_SECOND_PART_DIGITS = 6
	TIME_MAX_VALUE          = (TIME_MAX_HOUR*10000 + TIME_MAX_MINUTE*100 + TIME_MAX_SECOND)
	TIME_MAX_VALUE_SECONDS  = (TIME_MAX_HOUR*3600 +
		TIME_MAX_MINUTE*60 + TIME_MAX_SECOND)
)

func (c *Column) decodeTimeHiresDefault(scale int32) (err error) {
	/*
		Unpack the default value for a MariaDB TIME(N) column
		This is similar in function to the MYSQL_TYPE_TIME2 type,
		but is encoded as a standard MYSQL_TYPE_TIME field and
		values are unpacked according to the following logic.
	*/
	if scale > int32(len(TIME_HIRES_BYTES))-1 {
		return fmt.Errorf("invalid scale %d for TIME(N)", scale)
	}
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	nBytes := TIME_HIRES_BYTES[scale]
	var value uint64
	switch nBytes {
	case 3:
		value = uint64(utils.Uint24BE(data))
	case 4:
		value = uint64(binary.BigEndian.Uint32(data))
	case 5:
		value = utils.Uint40BE(data)
	case 6:
		value = utils.Uint48BE(data)
	}
	zeroPoint := secPartShift(
		(TIME_MAX_VALUE_SECONDS+1)*TIME_SECOND_PART_FACTOR,
		int(scale),
	)
	value = uint64(secPartUnshift(
		int(value-uint64(zeroPoint)),
		int(scale)),
	)
	usec := value % 1000000
	value /= 1000000
	sec := value % 60
	value /= 60
	minute := value % 60
	hour := value / 60
	result := fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, sec, usec)
	if scale < 6 {
		result = result[:len(result)-6+int(scale)]
	}
	c.Default = fmt.Sprintf("'%s'", result)
	return nil
}

func secPartShift(value int, digits int) int {
	return value / int(math.Pow10(TIME_SECOND_PART_DIGITS-digits))
}

func secPartUnshift(value int, digits int) int {
	return value * int(math.Pow10(TIME_SECOND_PART_DIGITS-digits))
}

func (c *Column) decodeTypeDatetime(hasDefault bool) error {
	scale := int32(c.Length) - MAX_DATETIME_WIDTH - 1
	if scale > 0 {
		c.TypeName += fmt.Sprintf("(%d)", scale)
	}
	if (c.TypeCode == MT_TIMESTAMP || c.TypeCode == MT_TIMESTAMP2) &&
		c.Flags.HasFlag(FF_MAYBE_NULL) {
		c.TypeName += " NULL"
	}
	if hasDefault {
		return c.decodeDatetimeDefault(scale)
	}
	return nil
}

func (c *Column) decodeDatetimeDefault(scale int32) (err error) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	switch c.TypeCode {
	case MT_TIMESTAMP:
		return c.decodeTimestampDefault(scale)
	case MT_TIMESTAMP2:
		return c.decodeTimestamp2Default(scale)
	case MT_DATETIME:
		if scale > 0 {
			c.decodeDatetimeHiresDefault(scale)
			return nil
		}
		value := binary.LittleEndian.Uint64(data)
		units := []struct {
			name  string
			zfill int
		}{
			{"second", 2},
			{"minute", 2},
			{"hour", 2},
			{"day", 2},
			{"month", 2},
			{"year", 4},
		}
		kwargs := make(map[string]string)
		for _, unit := range units {
			unitValue := value % uint64(math.Pow10(unit.zfill))
			value /= uint64(math.Pow10(unit.zfill))
			kwargs[unit.name] = fmt.Sprintf("%0*d", unit.zfill, unitValue)
		}
		c.Default = fmt.Sprintf("'%s-%s-%s %s:%s:%s'", kwargs["year"], kwargs["month"], kwargs["day"], kwargs["hour"], kwargs["minute"], kwargs["second"])
	case MT_DATETIME2:
		c.decodeDatetime2Default(scale)
	}
	return nil
}

func (c *Column) decodeDatetime2Default(scale int32) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	ymdhms := utils.Uint40BE(data)
	ymd := ymdhms >> 17
	ym := (ymd >> 5) & (1<<17 - 1)
	day := ymd & (1<<5 - 1)
	month := ym % 13
	year := ym / 13

	hms := ymdhms & (1<<17 - 1)
	second := hms & (1<<6 - 1)
	minute := (hms >> 6) & (1<<6 - 1)
	hour := hms >> 12

	// Format the datetime string
	value := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)

	if scale > 0 {
		nBytes := utils.DigitsToBytes[scale]
		fracBytes := data[5 : 5+nBytes]
		// Read fractional seconds
		padding := bytes.Repeat([]byte{0x00}, 4-len(fracBytes))
		microseconds := binary.BigEndian.Uint32(append(padding, fracBytes...))
		microStr := fmt.Sprintf("%0*d", scale, microseconds)
		if len(microStr) > int(scale) {
			microStr = microStr[:scale]
		}
		value += "." + microStr
	}
	c.Default = fmt.Sprintf("'%s'", value)
}

func (c *Column) decodeTimestampDefault(scale int32) error {
	if scale > 0 {
		return c.decodeTimestamp2Default(scale)
	}
	switch c.Utype {
	case UT_TIMESTAMP_DN_FIELD:
		c.Default = "CURRENT_TIMESTAMP"
	case UT_TIMESTAMP_UN_FIELD:
		c.Default = fmt.Sprintf("'%s' ON UPDATE CURRENT_TIMESTAMP", c.decodeTimestampDefaultValue())
	case UT_TIMESTAMP_DNUN_FIELD:
		c.Default = "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	default:
		c.Default = fmt.Sprintf("'%s'", c.decodeTimestampDefaultValue())
	}
	return nil
}

func (c *Column) decodeTimestampDefaultValue() string {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	epoch := int32(binary.BigEndian.Uint32(data))
	var value string
	if epoch != 0 {
		value = time.Unix(int64(epoch), 0).Format("2006-01-02 15:04:05")
	} else {
		value = "0000-00-00 00:00:00"
	}
	return value
}

func (c *Column) decodeTimestamp2Default(scale int32) error {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	epoch := int32(binary.BigEndian.Uint32(data))

	var value string
	if epoch != 0 {
		value = time.Unix(int64(epoch), 0).Format("2006-01-02 15:04:05")
	} else {
		value = "0000-00-00 00:00:00"
	}

	if scale > 0 {
		nBytes := utils.DigitsToBytes[scale] // Assuming digitsToBytes maps scale to byte count
		var fractional uint32
		switch nBytes {
		case 1:
			fractional = uint32(data[0])
		case 2:
			fractional = uint32(binary.BigEndian.Uint16(data))
		case 3:
			fractional = utils.Uint24BE(data)
		default:
			return fmt.Errorf("invalid scale %d for TIMESTAMP2", scale)
		}
		value += fmt.Sprintf(".%0*d", scale, fractional)
	}

	scaleStr := ""
	if scale > 0 {
		scaleStr = fmt.Sprintf("(%d)", scale)
	}

	switch c.Utype {
	case UT_TIMESTAMP_DN_FIELD:
		c.Default = fmt.Sprintf("CURRENT_TIMESTAMP%s", scaleStr)
	case UT_TIMESTAMP_UN_FIELD:
		c.Default = fmt.Sprintf("'%s' ON UPDATE CURRENT_TIMESTAMP%s", value, scaleStr)
	case UT_TIMESTAMP_DNUN_FIELD:
		c.Default = fmt.Sprintf("CURRENT_TIMESTAMP%s ON UPDATE CURRENT_TIMESTAMP%s", scaleStr, scaleStr)
	default:
		c.Default = fmt.Sprintf("'%s'", value)
	}
	return nil
}

func (c *Column) decodeDatetimeHiresDefault(scale int32) {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	value := binary.LittleEndian.Uint64(data)
	value = uint64(secPartUnshift(int(value), int(scale)))
	units := []struct {
		name  string
		unit  uint64
		zfill int
	}{
		{"usec", 1000000, int(scale)},
		{"second", 60, 2},
		{"minute", 60, 2},
		{"hour", 24, 2},
		{"day", 32, 2},
		{"month", 13, 2},
	}

	kwargs := make(map[string]string)
	var component uint64
	for _, unit := range units {
		value, component = value/unit.unit, value%unit.unit
		kwargs[unit.name] = fmt.Sprintf("%0*d", unit.zfill, component)
	}
	kwargs["year"] = fmt.Sprintf("%d", value)

	c.Default = fmt.Sprintf("'%s-%s-%s %s:%s:%s.%s'", kwargs["year"], kwargs["month"], kwargs["day"], kwargs["hour"], kwargs["minute"], kwargs["second"], kwargs["usec"])
}

func (c *Column) decodeTypeYear(hasDefault bool) {
	c.TypeName += fmt.Sprintf("(%d)", c.Length)
	if hasDefault {
		c.decodeYearDefault()
	}
}

func (c *Column) decodeYearDefault() {
	data := c.Defaults.Data[c.Defaults.CurrentOffset:]
	value := int(data[0]) + 1900
	c.Default = fmt.Sprintf("'%d'", value)
}

func (c *Column) decodeTypeDate(hasDefault bool) error {
	// these types have no additional type information, just type name
	if hasDefault {
		return c.decodeDateDefault()
	}
	return nil
}

func (c *Column) decodeDateDefault() error {
	switch c.TypeCode {
	case MT_DATE:
		return fmt.Errorf("pre 4.1 - unsupported for now, should be rare")
	case MT_NEWDATE:
		data := c.Defaults.Data[c.Defaults.CurrentOffset:]
		value := utils.Uint24LE(data)
		year := value >> 9
		month := (value >> 5) & 0xF
		day := value & 0x1F
		c.Default = fmt.Sprintf("'%04d-%02d-%02d'", year, month, day)
	}
	return nil
}

func (c *Column) decodeTypeGeometry(hasDefault bool) (err error) {
	c.TypeName, err = c.SubTypeCode.Name()
	if err != nil {
		return err
	}
	if hasDefault {
		return c.decodeGeometryDefault()
	}
	return nil
}

func (c *Column) decodeGeometryDefault() error {
	// GEOMETRY cannot have a default value so this should never be called
	return fmt.Errorf("GEOMETRY columns cannot have a default value")
}
