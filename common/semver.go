package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SemVer represents a semantic version
type SemVer struct {
	Version  int64
	Minor    int64
	Fix      int64
	Infinity bool
}

// ParseSemVer parses a semantic version string and returns a SemVer
func ParseSemVer(input string) (*SemVer, error) {
	trimmed := strings.TrimSpace(input)
	if len(trimmed) == 0 {
		return nil, errors.New("A semantic version can not be an empty string")
	}
	if trimmed == "INFINITY" {
		return &SemVer{-1, -1, -1, true}, nil
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) > 3 {
		return nil, fmt.Errorf("A semantic version (%s) must only have three parts, separated by periods", trimmed)
	}

	vmf := [3]int64{0, 0, 0}
	for index, value := range parts {
		if value != "0" {
			if strings.HasPrefix(value, "0") {
				return nil, fmt.Errorf("A semantic value component (%s) must not start with a zero", value)
			}
			for _, subValue := range value {
				if !strings.Contains("0123456789", string(subValue)) {
					return nil, fmt.Errorf("A semantic value component (%s) must be a number", value)
				}
			}
		}
		temp, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("A semantic value component (%s) must be a number", value)
		}
		vmf[index] = temp
	}
	return &SemVer{vmf[0], vmf[1], vmf[2], false}, nil
}

// String converts a SemVer to a string
func (semVer *SemVer) String() string {
	if semVer.Infinity {
		return "INFINITY"
	}
	return fmt.Sprintf("%d.%d.%d", semVer.Version, semVer.Minor, semVer.Fix)
}

// Compare compares a pair of SemVers
func (semVer *SemVer) Compare(b *SemVer) int {
	if semVer.Infinity {
		if b.Infinity {
			return 0
		}
		return 1
	} else if b.Infinity {
		return -1
	}

	if semVer.Version > b.Version {
		return 1
	} else if semVer.Version == b.Version {
		if semVer.Minor > b.Minor {
			return 1
		}
		if semVer.Minor == b.Minor {
			if semVer.Fix > b.Fix {
				return 1
			}
			if semVer.Fix == b.Fix {
				return 0
			}
			return -1
		}
		return -1
	}
	return -1
}

// SemVerRange represents a range over semantic version
type SemVerRange struct {
	start          *SemVer
	end            *SemVer
	startInclusive bool
	endInclusive   bool
}

// ParseSemVerRange parses a semantic version range string and returns a SemVerRange
func ParseSemVerRange(input string) (*SemVerRange, error) {
	trimmed := strings.TrimSpace(input)
	if len(trimmed) == 0 {
		return nil, errors.New("A semantic version range can not be an empty string")
	}
	lastChar := trimmed[len(trimmed)-1]

	var startInclusive, endInclusive bool

	if trimmed[0] == '[' {
		startInclusive = true
	} else if trimmed[0] == '(' {
		startInclusive = false
	} else {
		if lastChar == ']' || lastChar == ')' {
			return nil, fmt.Errorf("The semantic version range %s has no start character, but has an end character", trimmed)
		}
		semVer, err := ParseSemVer(trimmed)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse sematic version range %s. Error: %s", trimmed, err.Error())
		}
		if semVer.Infinity {
			return nil, errors.New("A semantic version range of one value can not be INFIITY")
		}
		return &SemVerRange{semVer, semVer, true, true}, nil
	}

	if lastChar == ']' {
		endInclusive = true
	} else if lastChar == ')' {
		endInclusive = false
	} else {
		return nil, fmt.Errorf("The semantic version range %s has no end character, but has a start character", trimmed)
	}

	parts := strings.Split(string(trimmed[1:len(trimmed)-1]), ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("The semantic version range %s has only one semantic range in it", trimmed)
	}

	start, err := ParseSemVer(parts[0])
	if err != nil {
		return nil, fmt.Errorf("The start of the semantic version range %s failed to parse. Error: %s", parts[0], err.Error())
	}
	if start.Infinity {
		return nil, errors.New("The start of a semantic version range can not be INFIITY")
	}

	end, err := ParseSemVer(parts[1])
	if err != nil {
		return nil, fmt.Errorf("The end of the semantic version range %s failed to parse. Error: %s", parts[1], err.Error())
	}

	return &SemVerRange{start, end, startInclusive, endInclusive}, nil
}

// IsInRange returns true/false if the SemVer is in the range of the SemVerRange
func (semVerRange *SemVerRange) IsInRange(semVer *SemVer) bool {
	comparison := semVerRange.start.Compare(semVer)
	if comparison > 0 || (!semVerRange.startInclusive && comparison == 0) {
		return false
	}

	comparison = semVer.Compare(semVerRange.end)
	if comparison < 0 || (semVerRange.endInclusive && comparison == 0) {
		return true
	}

	return false
}
