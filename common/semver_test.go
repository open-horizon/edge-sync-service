package common

import "testing"

func TestSemVer(t *testing.T) {
	tests := []struct {
		input    string
		expected *SemVer
		inError  bool
	}{
		{"1.0.0", &SemVer{1, 0, 0, false}, false}, {"INFINITY", &SemVer{-1, -1, -1, true}, false},
		{"1.0", &SemVer{1, 0, 0, false}, false}, {"0", &SemVer{0, 0, 0, false}, false},
		{"", &SemVer{-1, -1, -1, false}, true}, {"1.01", &SemVer{-1, -1, -1, false}, true},
		{"1.1.X", &SemVer{-1, -1, -1, false}, true}, {"23.42.1", &SemVer{23, 42, 1, false}, false},
	}

	for _, test := range tests {
		semVer, err := ParseSemVer(test.input)
		if test.inError && err == nil {
			t.Errorf("Failed to see an error in the sem ver %s.", test.input)
		} else if !test.inError && err != nil {
			t.Errorf("Failed to parse the sem ver %s.", test.input)
		}
		if err == nil {
			if test.expected.Infinity {
				if !semVer.Infinity {
					t.Errorf("The sem ver %s was suppose to be INFINITY", test.input)
				}
			} else {
				if semVer.Infinity {
					t.Errorf("The sem ver %s was not suppose to be INFINITY", test.input)
				}

				if test.expected.Version != semVer.Version || test.expected.Minor != semVer.Minor ||
					test.expected.Fix != semVer.Fix {
					t.Errorf("The sem ver %s did not parse as expected. Parsed as %d.%d.%d",
						test.input, semVer.Version, semVer.Minor, semVer.Fix)
				}
			}
		}
	}
}

func TestCompareSemVer(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected int
	}{
		{"1.0.0", "1.0.0", 0}, {"1.0", "1.0.0", 0}, {"1.0.0", "1.0", 0}, {"1", "1.0", 0},
		{"1.0.0", "2.0.0", -1}, {"1.0.0", "1.1.0", -1}, {"1.0.0", "1.0.1", -1},
		{"2.0.0", "1.0.0", 1}, {"1.1.0", "1.0.0", 1}, {"1.0.1", "1.0.0", 1},
		{"2.0", "1.2.3", 1}, {"1.2.3", "2.0", -1}, {"1.0", "1.0.1", -1},
		{"1.2.3", "INFINITY", -1}, {"INFINITY", "INFINITY", 0}, {"INFINITY", "23.12.42", 1},
	}

	for _, test := range tests {
		if a, err := ParseSemVer(test.a); err != nil {
			t.Errorf("Failed to parse left side sem ver (%s). Error: %s", test.a, err.Error())
		} else if b, err := ParseSemVer(test.b); err != nil {
			t.Errorf("Failed to parse right side sem ver (%s). Error: %s", test.b, err.Error())
		} else {
			if result := a.Compare(b); result != test.expected {
				t.Errorf("The compare of %s with %s failed. Result: %d", test.a, test.b, test.expected)
			}
		}
	}
}

func TestSemVerRange(t *testing.T) {
	tests := []struct {
		input    string
		expected *SemVerRange
		inError  bool
	}{
		{"1.0.0", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{1, 0, 0, false}, true, true}, false},
		{"[1.0.0, 1.2.0]", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{1, 2, 0, false}, true, true}, false},
		{"[1.0.0, 1.2.0)", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{1, 2, 0, false}, true, false}, false},
		{"(1.0.0, 1.2.0]", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{1, 2, 0, false}, false, true}, false},
		{"(1.0.0, 1.2.0)", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{1, 2, 0, false}, false, false}, false},
		{"(1.0.0, INFINITY)", &SemVerRange{&SemVer{1, 0, 0, false}, &SemVer{0, 0, 0, true}, false, false}, false},
		{"(INFINITY, 2.3.0)", nil, true}, {"[1.0.0]", nil, true}, {"(1.0.0)", nil, true},
		{"1.0.X", nil, true}, {"INFINITY", nil, true}, {"[1.0.0", nil, true}, {"(1.0.0", nil, true},
		{"1.0.0]", nil, true}, {"1.0.0)", nil, true}, {"1.0.0, 1.2.0", nil, true},
	}

	for _, test := range tests {
		semVerRange, err := ParseSemVerRange(test.input)
		if err != nil {
			if !test.inError {
				t.Errorf("Failed to parse the sem ver range %s. Error: %s", test.input, err.Error())
			}
		} else {
			if test.inError {
				t.Errorf("Parsed the sem ver range %s even though it was in error", test.input)
			} else {
				if 0 != semVerRange.start.Compare(test.expected.start) {
					t.Errorf("The start of the sem ver range %s didn't match the expected value %s", semVerRange.start, test.expected.start)
				}
			}
		}
	}
}

func TestSemVerInRange(t *testing.T) {
	tests := []struct {
		rangeInput  string
		semVerInput string
		expected    bool
	}{
		{"1.0.0", "1.0.0", true}, {"1.0.0", "1.0.2", false}, {"1.2.0", "1.0.2", false},
		{"[1.0.0, 1.2.0)", "1.0.1", true}, {"[1.0.0, 1.2.0)", "1.2.0", false},
		{"[1.0.0, 1.2.0]", "1.2.0", true}, {"(1.0.0, 1.2.0]", "1.2.0", true},
		{"[1.0.0, 1.2.0]", "1.0.0", true},
		{"(1.0.0, 1.2.0)", "1.0.1", true}, {"(1.0.0, 1.2.0)", "1.0.0", false},
		{"[1.0.0, 1.2.0)", "0.0.1", false}, {"[1.0.0, INFINITY)", "1.0.1", true},
	}

	for _, test := range tests {
		semVerRange, _ := ParseSemVerRange(test.rangeInput)
		semVer, _ := ParseSemVer(test.semVerInput)

		result := semVerRange.IsInRange(semVer)
		if result != test.expected {
			t.Errorf("The result of '%s'.IsInRange(%s) returned %t instead of %t",
				test.rangeInput, test.semVerInput, result, test.expected)
		}
	}
}
