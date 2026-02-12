package common

import (
	"testing"
)

// TestIsValidName tests validation of object and destination names:
// - Valid names with alphanumeric characters
// - Valid names with special characters (@, _, !, $, *, -)
// - Valid names with mixed case
// - Valid names with Unicode characters (e.g., Cyrillic 'ґ')
//
// This ensures that the name validation function accepts a wide range of
// valid characters while maintaining compatibility with various naming conventions.
// Names are used for objects, destinations, and other identifiers throughout the system.
func TestIsValidName(t *testing.T) {
	validNames := []string{"mercury", "V3NUS", "e@rth", "mARs", "jup_!teґ", "$aturn", "ur@n*$", "nep-tune", "plU1^"}

	for _, name := range validNames {
		if !IsValidName(name) {
			t.Errorf("Valid name %v was determined to be invalid.", name)
		}
	}
}
