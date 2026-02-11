package common

import (
	"testing"
)

func BenchmarkClearByteSlice(b *testing.B) {
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		copy(data, []byte("sensitive-data-that-needs-clearing"))
		ClearByteSlice(data)
	}
}

func BenchmarkClearString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		str := "test-string-abc"
		ClearString(&str)
	}
}

func BenchmarkSecureBytes_Clear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sb := NewSecureBytes([]byte("test-data-456"))
		sb.Clear()
	}
}

func BenchmarkSecureBytes_Create(b *testing.B) {
	data := []byte("test-data-456")
	for i := 0; i < b.N; i++ {
		_ = NewSecureBytes(data)
	}
}

func BenchmarkSecureBytes_CreateAndClear(b *testing.B) {
	data := []byte("test-data-456")
	for i := 0; i < b.N; i++ {
		sb := NewSecureBytes(data)
		sb.Clear()
	}
}

func BenchmarkSecureString_Clear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ss := NewSecureString("test-password-123")
		ss.Clear()
	}
}

func BenchmarkSecureString_Create(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewSecureString("test-password-123")
	}
}

func BenchmarkSecureString_CreateAndClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ss := NewSecureString("test-password-123")
		ss.Clear()
	}
}
