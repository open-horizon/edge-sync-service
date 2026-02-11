package common

import (
	"crypto/subtle"
)

// SecureString wraps sensitive string data that should be cleared from memory
type SecureString struct {
	data []byte
}

// NewSecureString creates a new SecureString from a regular string
func NewSecureString(s string) *SecureString {
	return &SecureString{data: []byte(s)}
}

// String returns the string value (use sparingly)
func (ss *SecureString) String() string {
	if ss.data == nil {
		return ""
	}
	return string(ss.data)
}

// Bytes returns the byte slice (use sparingly)
func (ss *SecureString) Bytes() []byte {
	return ss.data
}

// Clear securely wipes the sensitive data from memory
func (ss *SecureString) Clear() {
	if ss.data != nil {
		for i := range ss.data {
			ss.data[i] = 0
		}
		ss.data = nil
	}
}

// Compare securely compares two SecureStrings using constant-time comparison
func (ss *SecureString) Compare(other *SecureString) bool {
	if ss.data == nil || other.data == nil {
		return ss.data == nil && other.data == nil
	}
	return subtle.ConstantTimeCompare(ss.data, other.data) == 1
}

// SecureBytes wraps sensitive byte data
type SecureBytes struct {
	data []byte
}

// NewSecureBytes creates a new SecureBytes
func NewSecureBytes(b []byte) *SecureBytes {
	// Make a copy to avoid external modifications
	data := make([]byte, len(b))
	copy(data, b)
	return &SecureBytes{data: data}
}

// Bytes returns the byte slice
func (sb *SecureBytes) Bytes() []byte {
	return sb.data
}

// Clear securely wipes the data from memory
func (sb *SecureBytes) Clear() {
	if sb.data != nil {
		for i := range sb.data {
			sb.data[i] = 0
		}
		sb.data = nil
	}
}

// ClearByteSlice securely clears a byte slice
func ClearByteSlice(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// ClearString attempts to clear a string from memory (limited effectiveness due to Go's string immutability)
func ClearString(s *string) {
	if s != nil {
		// Convert to byte slice and clear
		b := []byte(*s)
		ClearByteSlice(b)
		*s = ""
	}
}
