package utils

import "strings"

var DigitsToBytes = [...]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func Uint24LE(b []byte) uint32 {
	if len(b) < 3 {
		return 0
	}
	return uint32(b[0]) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16)
}

func Uint24BE(b []byte) uint32 {
	if len(b) < 3 {
		return 0
	}
	return uint32(b[2]) | (uint32(b[1]) << 8) | (uint32(b[0]) << 16)
}

func Uint40LE(b []byte) uint64 {
	if len(b) < 5 {
		return 0
	}
	return uint64(b[0]) | (uint64(b[1]) << 8) | (uint64(b[2]) << 16) | (uint64(b[3]) << 24) | (uint64(b[4]) << 32)
}

func Uint40BE(b []byte) uint64 {
	if len(b) < 5 {
		return 0
	}
	return uint64(b[4]) | (uint64(b[3]) << 8) | (uint64(b[2]) << 16) | (uint64(b[1]) << 24) | (uint64(b[0]) << 32)
}

func Uint48LE(b []byte) uint64 {
	if len(b) < 6 {
		return 0
	}
	return uint64(b[0]) | (uint64(b[1]) << 8) | (uint64(b[2]) << 16) | (uint64(b[3]) << 24) | (uint64(b[4]) << 32) | (uint64(b[5]) << 40)
}

func Uint48BE(b []byte) uint64 {
	if len(b) < 6 {
		return 0
	}
	return uint64(b[5]) | (uint64(b[4]) << 8) | (uint64(b[3]) << 16) | (uint64(b[2]) << 24) | (uint64(b[1]) << 32) | (uint64(b[0]) << 40)
}

func CalculateDecimalLengths(precision, scale int) (intLength, fracLength int) {
	intLength = ((precision-scale)/9)*4 + DigitsToBytes[(precision-scale)%9]
	fracLength = (scale/9)*4 + DigitsToBytes[scale%9]
	return
}

// zfill takes a string and pads it with zeros on the left to the desired length
func Zfill(number string, scale int) string {
	padSize := scale - len(number)
	if padSize > 0 {
		return strings.Repeat("0", padSize) + number
	}
	return number
}
