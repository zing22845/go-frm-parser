package view

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

func Parse(path string, data string) (view *MySQLView, err error) {
	view = &MySQLView{}

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]

		switch key {
		case "query":
			view.Body = unescape(value)
		case "md5":
			view.StoredMD5 = value
		case "updatable":
			// Ignore this field
		case "algorithm":
			view.Algorithm = parseAlgorithm(value)
		case "definer_user":
			view.Definer.User = value
		case "definer_host":
			view.Definer.Host = value
		case "suid":
			view.SUID = parseSUID(value)
		case "with_check_option":
			view.CheckOption = parseCheckOption(value)
		case "timestamp":
			view.Timestamp, err = time.Parse("2006-01-02 15:04:05", value)
			if err != nil {
				return view, err
			}
		case "create-version":
			// Ignore this field
		case "source":
			// Ignore this field
		case "client_cs_name":
			// Ignore this field
		case "connection_cl_name":
			// Ignore this field
		case "view_body_utf8":
			// Ignore this field
		}
	}

	view.ParseName(path)
	view.ComputedMD5 = computeMD5(view.Body)
	if view.StoredMD5 != view.ComputedMD5 {
		return view, fmt.Errorf(
			"md5 not match, stored: %s, computed: %s",
			view.StoredMD5, view.ComputedMD5)
	}

	return view, nil
}

func unescape(value string) string {
	metaMapping := map[string]string{
		"b":  "\\b",
		"t":  "\\t",
		"n":  "\\n",
		"r":  "\\r",
		"\\": "\\\\",
		"s":  " ",
		"\"": "\"",
		"'":  "'",
	}

	regex := regexp.MustCompile(`\\(\['"btnr\\s])`)
	return regex.ReplaceAllStringFunc(value, func(match string) string {
		return metaMapping[match[1:2]]
	})
}

func parseAlgorithm(input string) Algorithm {
	switch input {
	case "0":
		return Undefined
	case "1":
		return TmpTable
	case "2":
		return Merge
	default:
		return Undefined
	}
}

func parseSUID(input string) SUIDType {
	switch input {
	case "0":
		return Invoker
	case "1":
		return Definer
	case "2":
		return Default
	default:
		return Invoker
	}
}

func parseCheckOption(input string) CheckOption {
	switch input {
	case "0":
		return None
	case "1":
		return Local
	case "2":
		return Cascaded
	default:
		return None
	}
}

func computeMD5(input string) string {
	hash := md5.New()
	_, _ = io.WriteString(hash, input)
	return hex.EncodeToString(hash.Sum(nil))
}
