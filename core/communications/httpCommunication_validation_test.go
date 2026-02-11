package communications

import (
	"testing"
)

func TestValidateURLParameter_ControlCharacters(t *testing.T) {
	controlChars := []string{
		"user\nname",
		"user\rname",
		"user\tname",
	}

	for _, attack := range controlChars {
		if err := validateURLParameter(attack); err == nil {
			t.Errorf("Control character should be blocked: %s", attack)
		}
	}
}

func TestValidateURLParameter_EmptyString(t *testing.T) {
	// Empty string should fail (no valid characters)
	if err := validateURLParameter(""); err == nil {
		t.Error("Empty string should be rejected")
	}
}

func TestValidateURLParameter_Multilingual_Valid(t *testing.T) {
	validInputs := []string{
		"user-123",         // English
		"用户_456",           // Chinese
		"مستخدم.789",       // Arabic
		"пользователь-123", // Cyrillic
		"משתמש_456",        // Hebrew
		"ユーザー.789",         // Japanese
		"café-123",         // French (with accent)
		"naïve_user",       // Combining marks
		"test.org-123",     // Mixed with dots and dashes
		"Ñoño_123",         // Spanish
		"Müller-456",       // German
		"São_Paulo",        // Portuguese
	}

	for _, input := range validInputs {
		if err := validateURLParameter(input); err != nil {
			t.Errorf("Valid multilingual input should pass: %s, error: %v", input, err)
		}
	}
}

func TestValidateURLParameter_PathTraversal(t *testing.T) {
	pathTraversals := []string{
		"../etc/passwd",
		"..\\windows\\system32",
		"....//....//etc/passwd",
		"path/../../../etc/passwd",
	}

	for _, attack := range pathTraversals {
		if err := validateURLParameter(attack); err == nil {
			t.Errorf("Path traversal should be blocked: %s", attack)
		}
	}
}

func TestValidateURLParameter_SpecialCharacters(t *testing.T) {
	specialChars := []string{
		"user@domain",
		"user#tag",
		"user$money",
		"user%percent",
		"user&and",
		"user*star",
		"user+plus",
		"user=equals",
		"user[bracket",
		"user]bracket",
		"user{brace",
		"user}brace",
		"user|pipe",
		"user\\backslash",
		"user/slash",
		"user?question",
		"user!exclaim",
		"user~tilde",
		"user`backtick",
		"user;semicolon",
		"user:colon",
		"user'quote",
		"user\"doublequote",
		"user<less",
		"user>greater",
		"user,comma",
	}

	for _, input := range specialChars {
		if err := validateURLParameter(input); err == nil {
			t.Errorf("Special character should be blocked: %s", input)
		}
	}
}

func TestValidateURLParameter_URLSchemes(t *testing.T) {
	urlSchemes := []string{
		"http://evil.com",
		"https://malicious.org",
		"ftp://server.com",
		"file:///etc/passwd",
	}

	for _, attack := range urlSchemes {
		if err := validateURLParameter(attack); err == nil {
			t.Errorf("URL scheme should be blocked: %s", attack)
		}
	}
}

func TestValidateURLParameter_ValidASCII(t *testing.T) {
	validInputs := []string{
		"user123",
		"test-user",
		"user_name",
		"test.org",
		"a1b2c3",
		"MyUser-123",
	}

	for _, input := range validInputs {
		if err := validateURLParameter(input); err != nil {
			t.Errorf("Valid ASCII input should pass: %s, error: %v", input, err)
		}
	}
}

func TestValidateURLParameter_XSS_Attacks(t *testing.T) {
	xssAttacks := []string{
		"<script>alert('xss')</script>",
		"<img src=x onerror=alert('xss')>",
		"javascript:alert('xss')",
		"<svg onload=alert('xss')>",
		"'><script>alert('xss')</script>",
		"\"><script>alert('xss')</script>",
		"<iframe src='javascript:alert(\"xss\")'></iframe>",
		"<body onload=alert('xss')>",
		"<input onfocus=alert('xss') autofocus>",
		"<select onfocus=alert('xss') autofocus>",
	}

	for _, attack := range xssAttacks {
		if err := validateURLParameter(attack); err == nil {
			t.Errorf("XSS attack should be blocked: %s", attack)
		}
	}
}
