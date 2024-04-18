package table

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// /* The combination of the above can be used for key type comparison. */
// #define HA_KEYFLAG_MASK (HA_NOSAME | HA_PACK_KEY | HA_AUTO_KEY |
//                          HA_BINARY_PACK_KEY | HA_FULLTEXT | HA_UNIQUE_CHECK |
//                         HA_SPATIAL | HA_NULL_ARE_EQUAL | HA_GENERATED_KEY)

type Key struct {
	Name       string
	PartsCount uint8
	Parts      []*KeyPart
	Algorithm  HaKeyAlgo
	BlockSize  uint16
	Parser     string
	Comment    string
	IndexType  string
	IsUnique   bool
	Keys       *Keys
	Columns    *Columns
}

type KeyPart struct {
	Column *Column
	Length uint16
}

func (k *Key) String() string {
	components := make([]string, 0)
	if k.Name == "PRIMARY" {
		components = append(components, "PRIMARY KEY")
	} else if k.IsUnique {
		components = append(components, "UNIQUE KEY")
	} else if k.IndexType == "FULLTEXT" {
		components = append(components, "FULLTEXT KEY")
	} else if k.IndexType == "SPATIAL" {
		components = append(components, "SPATIAL KEY")
	} else {
		components = append(components, "KEY")
	}

	if k.Name != "" && k.Name != "PRIMARY" {
		components = append(components, fmt.Sprintf("`%s`", k.Name))
	}

	var keyParts []string
	for _, part := range k.Parts {
		keyParts = append(keyParts, k.FormatKeyPart(part))
	}
	columns := fmt.Sprintf("(%s)", strings.Join(keyParts, ","))
	components = append(components, columns)

	if k.Algorithm != HA_KEY_ALG_UNDEF {
		components = append(components, fmt.Sprintf("USING %s", k.Algorithm.Name()))
	}
	if k.BlockSize > 0 {
		components = append(components, fmt.Sprintf("KEY_BLOCK_SIZE=%d", k.BlockSize))
	}
	if k.Comment != "" {
		components = append(components, fmt.Sprintf("COMMENT '%s'", k.Comment))
	}
	if k.Parser != "" && k.Parser != "True" { // Assuming 'True' is a placeholder for an undefined parser
		components = append(components, fmt.Sprintf("/*!50100 WITH PARSER `%s` */ ", k.Parser))
	}
	return strings.Join(components, " ")
}

func (k *Key) FormatKeyPart(part *KeyPart) string {
	// format the basic column name being indexed
	value := part.String()

	// Check if the index type is FULLTEXT or SPATIAL
	if k.IndexType == "FULLTEXT" || k.IndexType == "SPATIAL" {
		// FULLTEXT/SPATIAL may never have an index prefix
		return value
	}

	// get key prefix ignore error,
	// as the column type is already validated in column decoder
	keyPrefix, _ := part.Column.TypeCode.KeyPrefix()
	// Determine if a prefix is necessary based on column type
	if keyPrefix == KP_MAYBE && part.Length != part.Column.Length ||
		keyPrefix == KP_ALWAYS {
		prefixLength := part.Length / uint16(part.Column.Collation.Maxlen)
		value += fmt.Sprintf("(%d)", prefixLength)
	}
	return value
}

func (k *Key) Decode() {
	data := k.Keys.Data[k.Keys.CurrentOffset:]
	flags := HaKeyFlag(binary.LittleEndian.Uint16(data) ^ uint16(HA_NOSAME))
	// length := binary.LittleEndian.Uint16(data[2:]) // unused
	k.PartsCount = uint8(data[4])
	k.Algorithm = HaKeyAlgo(data[5])
	k.BlockSize = binary.LittleEndian.Uint16(data[6:])
	k.Keys.CurrentOffset += 8
	if flags.HasFlag(HA_USES_COMMENT) {
		k.Comment = k.Keys.Comments.Decode()
	}
	if flags.HasFlag(HA_USES_PARSER) {
		k.Parser = k.Keys.Extra.DecodeParser()
	}
	k.DecodeParts()

	if flags.HasFlag(HA_FULLTEXT) {
		k.IndexType = "FULLTEXT"
	} else if flags.HasFlag(HA_SPATIAL) {
		k.IndexType = "SPATIAL"
	} else if k.Algorithm == HA_KEY_ALG_HASH {
		k.IndexType = k.Algorithm.Name()
	} else {
		k.IndexType = "BTREE"
	}
}

func (kp *KeyPart) String() string {
	return fmt.Sprintf("`%s`", kp.Column.Name)
}

func (k *Key) DecodeParts() {
	k.Parts = make([]*KeyPart, k.PartsCount)
	for i := 0; i < int(k.PartsCount); i++ {
		data := k.Keys.Data[k.Keys.CurrentOffset:]
		fieldnr := binary.LittleEndian.Uint16(data) & 0x3fff // offset +2
		// _ = binary.LittleEndian.Uint16(data[2:]) - 1 // skip offset +4
		// _ = uint8(data[4])                           // skip flags  +5
		// _ = binary.LittleEndian.Uint16(data[5:])     // skip key_type +7
		length := binary.LittleEndian.Uint16(data[7:]) // offset +9
		k.Keys.CurrentOffset += 9
		column := k.Columns.Items[fieldnr-1]
		k.Parts[i] = &KeyPart{
			Column: column,
			Length: length,
		}
	}
}
