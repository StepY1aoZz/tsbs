package main

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/targets/opengemini/record"
	"github.com/valyala/fastjson/fastfloat"
	"math"
	"strings"
)

func parseFieldNumValue(s string) (float64, int32, error) {
	if len(s) == 0 {
		return 0, record.FieldTypeUnknown, fmt.Errorf("field value cannot be empty")
	}
	ch := s[len(s)-1]
	if ch == 'i' {
		// Integer value
		ss := s[:len(s)-1]
		n, err := fastfloat.ParseInt64(ss)
		if err != nil {
			return 0, record.FieldTypeUnknown, err
		}
		return float64(n), record.FieldTypeInt, nil
	}
	if ch == 'u' {
		// Unsigned integer value
		return 0, record.FieldTypeUnknown, fmt.Errorf("invalid number")
	}
	if ch == 'f' {
		// Unsigned integer value
		ss := s[:len(s)-1]
		n := fastfloat.ParseBestEffort(ss)
		return n, record.FieldTypeFloat, nil
	}
	if s == "t" || s == "T" || s == "true" || s == "True" || s == "TRUE" {
		return 1, record.FieldTypeBoolean, nil
	}
	if s == "f" || s == "F" || s == "false" || s == "False" || s == "FALSE" {
		return 0, record.FieldTypeBoolean, nil
	}

	if !IsValidNumber(s) {
		return 0, record.FieldTypeUnknown, fmt.Errorf("invalid field value")
	}

	f := fastfloat.ParseBestEffort(s)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, record.FieldTypeUnknown, fmt.Errorf("invalid number")
	}

	return f, record.FieldTypeFloat, nil
}

func parseFieldStrValue(s string) (string, error) {
	if len(s) == 0 {
		return "", fmt.Errorf("field value cannot be empty")
	}
	if s[0] == '"' {
		if len(s) < 2 || s[len(s)-1] != '"' {
			return "", fmt.Errorf("missing closing quote for quoted field value %s", s)
		}
		s = s[1 : len(s)-1]
		n := strings.IndexByte(s, '\\')
		if n < 0 {
			// no '\' escape chars
			return s, nil
		}
		// Try to unquote string, since sometimes insert escape chars.
		// s: "disk\" mem\\\" cpu\ host\\ server\\\"
		var ret strings.Builder
		for ; n >= 0; n = strings.IndexByte(s, '\\') {
			origN := n
			// count the slashes
			slashes := 1
			for n < len(s)-1 && s[n+1] == '\\' {
				slashes++
				n++
			}
			if n < len(s)-1 && s[n+1] == '"' {
				// next char is '"', no need keep one '/'
				ret.WriteString(s[:origN+slashes/2])
				ret.WriteByte('"')
				n++
			} else {
				// next char is not '"', keep one '/' at last
				if slashes&1 == 0 {
					ret.WriteString(s[:origN+slashes/2])
				} else {
					ret.WriteString(s[:origN+slashes/2+1])
				}
			}
			if n < len(s)-1 {
				s = s[n+1:]
				continue
			}
			s = ""
		}
		ret.WriteString(s)
		return ret.String(), nil
	}
	return "", nil
}
