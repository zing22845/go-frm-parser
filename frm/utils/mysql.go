package utils

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/pkg/parser/charset"
)

func UTF8Decoder(b []byte, charsetName string) (utf8Str string, err error) {
	enc := charset.FindEncoding(charsetName)
	utf8DecodedBytes, err := enc.Transform(nil, b, charset.OpDecode)
	if err != nil {
		return "", err
	}
	return string(utf8DecodedBytes), nil
}

// EncodeMySQLObject2File encodes a string to a format suitable for writing to a MySQL file
// https://dev.mysql.com/doc/refman/8.0/en/identifier-mapping.html
func EncodeMySQLObject2File(input string) string {
	encodedStr := ""
	for _, c := range input {
		if unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c) || c == '_' {
			// Append ASCII characters directly
			encodedStr += string(c)
		} else {
			// Convert non-ASCII characters to their Unicode code points in hexadecimal
			// Prefix the hexadecimal code with '@' and append to the result string
			// 0x0001 to 0xFFFF   https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
			encodedStr += fmt.Sprintf("@%04x", c)
		}
	}
	return encodedStr
}

// DecodeMySQLFile2Object decodes a string that was encoded using EncodeMySQLObject2File
// https://dev.mysql.com/doc/refman/8.0/en/identifier-mapping.html
func DecodeMySQLFile2Object(encodedStr string) (string, error) {
	// Split the input string into parts based on '@'
	parts := strings.Split(encodedStr, "@")
	// The first part is always the initial segment of the string
	decodedStr := parts[0]
	// Process each subsequent part
	for _, part := range parts[1:] {
		if len(part) < 4 {
			// If the part does not have enough characters, it is not a valid encoding
			return "", fmt.Errorf("invalid MySQL file encoding: %s", part)
		}
		// Extract the hexadecimal part
		hexPart := part[:4]
		restPart := part[4:]
		// Convert the hexadecimal string to an integer
		unicodeCodePoint, err := strconv.ParseInt(hexPart, 16, 32)
		if err != nil {
			return "", err
		}
		// Convert the integer to the corresponding Unicode character
		decodedChar := string(rune(unicodeCodePoint))
		// Append the decoded character and the rest of the part to the result
		decodedStr += decodedChar + restPart
	}
	return decodedStr, nil
}
