package common

import (
	"testing"
)

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

func TestClearByteSlice_Nil(t *testing.T) {
	// Test clearing nil slice
	var data []byte
	ClearByteSlice(data) // Should not panic
}

func TestClearString(t *testing.T) {
	str := "test-string-abc"
	ClearString(&str)

	if str != "" {
		t.Errorf("String not cleared: got %q, want empty string", str)
	}
}

func TestClearString_Nil(t *testing.T) {
	// Test clearing nil string pointer
	ClearString(nil) // Should not panic
}

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

func TestSecureBytes_EmptySlice(t *testing.T) {
	// Test clearing empty slice
	sb := NewSecureBytes([]byte{})
	sb.Clear() // Should not panic

	if sb.Bytes() != nil {
		t.Error("Empty SecureBytes should be nil after clear")
	}
}

func TestSecureString_Bytes(t *testing.T) {
	ss := NewSecureString("test-value")
	bytes := ss.Bytes()
	if string(bytes) != "test-value" {
		t.Errorf("Expected 'test-value', got %q", string(bytes))
	}
}

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

func TestSecureString_CompareNil(t *testing.T) {
	ss1 := NewSecureString("")
	ss1.Clear()

	ss2 := NewSecureString("")
	ss2.Clear()

	if !ss1.Compare(ss2) {
		t.Error("Expected cleared SecureStrings to compare as equal")
	}
}

func TestSecureString_MultipleClears(t *testing.T) {
	// Test that multiple clears don't cause issues
	ss := NewSecureString("password")
	ss.Clear()
	ss.Clear() // Should not panic

	if ss.String() != "" {
		t.Error("SecureString should remain empty after multiple clears")
	}
}

func TestSecureString_String(t *testing.T) {
	ss := NewSecureString("test-value")
	if ss.String() != "test-value" {
		t.Errorf("Expected 'test-value', got %q", ss.String())
	}
}
