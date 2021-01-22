package common

import (
	"testing"
)

func TestIsValidName(t *testing.T) {
	validNames := []string{"mercury", "V3NUS", "e@rth", "mARs", "jup_!teґ", "$aturn", "ur@n*$", "nep-tune", "plU1^"}

	for _, name := range validNames {
		if !IsValidName(name) {
			t.Errorf("Valid name %v was determined to be invalid.", name)
		}
	}
}
