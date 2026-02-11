package communications

import (
	"testing"
)

// TestValidateURLParameterMultilingual tests multilingual support in URL parameter validation
func TestValidateURLParameterMultilingual(t *testing.T) {
	validCases := []string{
		"user-123",         // English
		"用户_456",           // Chinese
		"مستخدم.789",       // Arabic
		"пользователь-123", // Cyrillic
		"משתמש_456",        // Hebrew
		"ユーザー.789",         // Japanese
		"café-123",         // French (with accent)
		"naïve_user",       // Combining marks
		"test.org-123",     // Mixed with dots and dashes
	}

	invalidCases := []string{
		"user<script>",    // XSS attempt
		"../etc/passwd",   // Path traversal
		"http://evil.com", // URL scheme
		"user\nname",      // Control character (newline)
		"user\tname",      // Control character (tab)
		"user\rname",      // Control character (carriage return)
		"test://scheme",   // URL scheme
		"path/../up",      // Path traversal
	}

	for _, tc := range validCases {
		if err := validateURLParameter(tc); err != nil {
			t.Errorf("Expected valid: %s, got error: %v", tc, err)
		}
	}

	for _, tc := range invalidCases {
		if err := validateURLParameter(tc); err == nil {
			t.Errorf("Expected invalid: %s, but passed validation", tc)
		}
	}
}
