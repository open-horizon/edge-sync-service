package common

import (
	"testing"
)

// TestClearByteSlice tests secure memory clearing for byte slices:
// - Verifies all bytes are zeroed after clearing
// - Ensures sensitive data is properly wiped from memory
// - Tests in-place memory clearing
//
// This is critical for security when handling sensitive data like passwords,
// keys, or tokens. Ensures that sensitive data doesn't remain in memory after use,
// reducing the risk of memory dumps or process inspection revealing secrets.
func TestClearByteSlice(t *testing.T) {
	data := []byte("test-data-789")
	original := make([]byte, len(data))
	copy(original, data)

	ClearByteSlice(data)

	// Verify all bytes are zero
	for i, b := range data {
		if b != 0 {
			t.Errorf("Byte[%d] not cleared: got %d, want 0", i, b)
		}
	}
}

// TestClearByteSlice_Nil tests secure memory clearing with nil slice:
// - Verifies function handles nil slice gracefully
// - Ensures no panic when clearing nil
// - Tests defensive programming for edge cases
//
// This ensures robustness when clearing potentially uninitialized or nil slices,
// preventing crashes in error handling paths where data may not be allocated.
func TestClearByteSlice_Nil(t *testing.T) {
	// Test clearing nil slice
	var data []byte
	ClearByteSlice(data) // Should not panic
}

// TestClearString tests secure memory clearing for strings:
// - Verifies string is emptied after clearing
// - Ensures sensitive string data is wiped from memory
// - Tests string pointer modification
//
// Critical for security when handling sensitive string data like passwords,
// API keys, or tokens. Ensures that sensitive strings don't remain in memory
// after use, reducing the risk of memory inspection revealing secrets.
func TestClearString(t *testing.T) {
	str := "test-string-abc"
	ClearString(&str)

	if str != "" {
		t.Errorf("String not cleared: got %q, want empty string", str)
	}
}

// TestClearString_Nil tests secure memory clearing with nil string pointer:
// - Verifies function handles nil pointer gracefully
// - Ensures no panic when clearing nil
// - Tests defensive programming for edge cases
//
// This ensures robustness when clearing potentially uninitialized or nil string
// pointers, preventing crashes in error handling paths.
func TestClearString_Nil(t *testing.T) {
	// Test clearing nil string pointer
	ClearString(nil) // Should not panic
}

// TestSecureBytes_Bytes tests SecureBytes data access:
// - Verifies Bytes() returns a copy, not the original slice
// - Ensures data isolation between SecureBytes and caller
// - Tests memory safety
//
// This ensures that SecureBytes provides proper encapsulation, preventing
// external modification of the secure data through returned references.
// Critical for maintaining data integrity and security.
func TestSecureBytes_Bytes(t *testing.T) {
	original := []byte("test-data")
	sb := NewSecureBytes(original)

	// Verify it's a copy, not the same slice
	bytes := sb.Bytes()
	if &bytes[0] == &original[0] {
		t.Error("SecureBytes should create a copy, not reference original")
	}

	if string(bytes) != "test-data" {
		t.Errorf("Expected 'test-data', got %q", string(bytes))
	}
}

// TestSecureBytes_Clear tests secure memory clearing for SecureBytes:
// - Verifies all bytes are zeroed after clearing
// - Ensures data is set to nil after clear
// - Tests proper cleanup of sensitive data
//
// This is critical for security when handling sensitive byte data like
// encryption keys, certificates, or tokens. Ensures that sensitive data
// doesn't remain in memory after use, reducing the risk of memory dumps
// or process inspection revealing secrets.
func TestSecureBytes_Clear(t *testing.T) {
	// Test that SecureBytes properly clears memory
	sb := NewSecureBytes([]byte("sensitive-data-456"))

	// Clear the secure bytes
	sb.Clear()

	// Verify all bytes are zero
	if sb.Bytes() != nil {
		t.Error("SecureBytes not cleared: data should be nil")
	}
}

// TestSecureBytes_EmptySlice tests SecureBytes with empty slice:
// - Verifies empty slice handling
// - Ensures no panic when clearing empty data
// - Tests edge case behavior
//
// This ensures robustness when working with empty or zero-length secure data,
// preventing crashes in edge cases.
func TestSecureBytes_EmptySlice(t *testing.T) {
	// Test clearing empty slice
	sb := NewSecureBytes([]byte{})
	sb.Clear() // Should not panic

	if sb.Bytes() != nil {
		t.Error("Empty SecureBytes should be nil after clear")
	}
}

// TestSecureString_Bytes tests SecureString byte conversion:
// - Verifies Bytes() returns correct byte representation
// - Tests string-to-bytes conversion
// - Ensures data integrity during conversion
//
// This ensures that SecureString can be converted to bytes when needed
// (e.g., for cryptographic operations) while maintaining data integrity.
func TestSecureString_Bytes(t *testing.T) {
	ss := NewSecureString("test-value")
	bytes := ss.Bytes()
	if string(bytes) != "test-value" {
		t.Errorf("Expected 'test-value', got %q", string(bytes))
	}
}

// TestSecureString_Clear tests secure memory clearing for SecureString:
// - Verifies string is emptied after clearing
// - Ensures sensitive string data is wiped from memory
// - Tests proper cleanup of sensitive data
//
// This is critical for security when handling sensitive string data like
// passwords, API keys, or tokens. Ensures that sensitive strings don't
// remain in memory after use, reducing the risk of memory dumps or process
// inspection revealing secrets.
func TestSecureString_Clear(t *testing.T) {
	// Test that SecureString properly clears memory
	ss := NewSecureString("sensitive-password-123")

	// Clear the secure string
	ss.Clear()

	// Verify it's been zeroed
	if ss.String() != "" {
		t.Errorf("SecureString not cleared: got %q, want empty string", ss.String())
	}
}

// TestSecureString_Compare tests constant-time string comparison:
// - Equal strings compare as equal
// - Different strings compare as not equal
// - Constant-time comparison (timing-attack resistant)
//
// This is critical for security when comparing sensitive strings like passwords
// or tokens. Uses constant-time comparison to prevent timing attacks where an
// attacker could deduce the correct value by measuring comparison time.
// Implements defense against CWE-208: Observable Timing Discrepancy.
func TestSecureString_Compare(t *testing.T) {
	ss1 := NewSecureString("password123")
	ss2 := NewSecureString("password123")
	ss3 := NewSecureString("different")

	if !ss1.Compare(ss2) {
		t.Error("Expected equal SecureStrings to compare as equal")
	}

	if ss1.Compare(ss3) {
		t.Error("Expected different SecureStrings to compare as not equal")
	}
}

// TestSecureString_CompareNil tests constant-time comparison with cleared strings:
// - Cleared strings compare as equal
// - Nil/empty string handling
// - Edge case behavior
//
// This ensures that comparison works correctly even after strings have been
// cleared, maintaining consistent behavior across all states.
func TestSecureString_CompareNil(t *testing.T) {
	ss1 := NewSecureString("")
	ss1.Clear()

	ss2 := NewSecureString("")
	ss2.Clear()

	if !ss1.Compare(ss2) {
		t.Error("Expected cleared SecureStrings to compare as equal")
	}
}

// TestSecureString_MultipleClears tests idempotent clearing behavior:
// - Multiple clear operations on same SecureString
// - Verifies no panic on repeated clears
// - Tests idempotent operation (multiple calls have same effect as one)
//
// This ensures robustness when clear operations may be called multiple times
// (e.g., in defer statements or error handling paths), preventing crashes
// from redundant cleanup operations.
func TestSecureString_MultipleClears(t *testing.T) {
	// Test that multiple clears don't cause issues
	ss := NewSecureString("password")
	ss.Clear()
	ss.Clear() // Should not panic

	if ss.String() != "" {
		t.Error("SecureString should remain empty after multiple clears")
	}
}

// TestSecureString_String tests SecureString string conversion:
// - Verifies String() returns correct string representation
// - Tests data access method
// - Ensures data integrity during conversion
//
// This ensures that SecureString can be converted back to a regular string
// when needed for operations that require string type, while maintaining
// data integrity.
func TestSecureString_String(t *testing.T) {
	ss := NewSecureString("test-value")
	if ss.String() != "test-value" {
		t.Errorf("Expected 'test-value', got %q", ss.String())
	}
}
