package utils

import (
	"strings"
)

func splitLineFunc(r rune) bool {
	return r == '\r' || r == '\n'
}

func SplitLine(str string) []string {
	return strings.FieldsFunc(str, splitLineFunc)
}
