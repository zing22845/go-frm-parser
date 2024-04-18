package utils

import "github.com/pingcap/tidb/pkg/parser/charset"

func UTF8Decoder(b []byte, charsetName string) (utf8Str string, err error) {
	enc := charset.FindEncoding(charsetName)
	utf8DecodedBytes, err := enc.Transform(nil, b, charset.OpDecode)
	if err != nil {
		return "", err
	}
	return string(utf8DecodedBytes), nil
}
