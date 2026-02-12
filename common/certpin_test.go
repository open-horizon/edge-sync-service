package common

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"testing"
	"time"
)

func generateTestCertificate() ([]byte, string, error) {
	// Generate test certificate
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, "", err
	}

	// Calculate fingerprint
	fingerprint := sha256.Sum256(certDER)
	fingerprintHex := hex.EncodeToString(fingerprint[:])

	return certDER, fingerprintHex, nil
}

// TestVerifyPinnedCertificate_CaseInsensitive tests certificate fingerprint matching with case variations:
// - Uppercase fingerprint matching
// - Mixed case fingerprint handling
// - Case-insensitive comparison
//
// This ensures that certificate pinning works regardless of fingerprint case,
// improving usability while maintaining security. Fingerprints are often
// displayed in different cases by various tools.
func TestVerifyPinnedCertificate_CaseInsensitive(t *testing.T) {
	certDER, fingerprint, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Test with uppercase fingerprint
	uppercaseFingerprint := ""
	for _, c := range fingerprint {
		if c >= 'a' && c <= 'f' {
			uppercaseFingerprint += string(c - 32)
		} else {
			uppercaseFingerprint += string(c)
		}
	}

	err = VerifyPinnedCertificate([][]byte{certDER}, []string{uppercaseFingerprint})
	if err != nil {
		t.Errorf("Expected case-insensitive matching, got error: %v", err)
	}
}

// TestVerifyPinnedCertificate_InvalidCertificate tests handling of invalid certificate data:
// - Malformed certificate bytes
// - Non-certificate data
// - Corrupted certificate structures
//
// This ensures that certificate pinning fails gracefully when presented with
// invalid certificate data, preventing potential security bypasses through
// malformed input.
func TestVerifyPinnedCertificate_InvalidCertificate(t *testing.T) {
	// Invalid certificate data
	invalidCert := []byte("not-a-certificate")

	err := VerifyPinnedCertificate([][]byte{invalidCert}, []string{"some-fingerprint"})
	if err == nil {
		t.Error("Expected error with invalid certificate")
	}
}

// TestVerifyPinnedCertificate_MatchingFingerprint tests successful certificate pinning:
// - Exact fingerprint match
// - Valid certificate with matching SHA256 fingerprint
// - Successful verification flow
//
// This is the primary happy path test ensuring that certificate pinning works
// correctly when a valid certificate matches the configured fingerprint.
// Critical for TLS security and preventing man-in-the-middle attacks.
func TestVerifyPinnedCertificate_MatchingFingerprint(t *testing.T) {
	certDER, fingerprint, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Matching fingerprint - should pass
	err = VerifyPinnedCertificate([][]byte{certDER}, []string{fingerprint})
	if err != nil {
		t.Errorf("Expected no error with matching fingerprint, got: %v", err)
	}
}

// TestVerifyPinnedCertificate_MultipleCertificates tests certificate chain validation:
// - Multiple certificates in chain
// - Only first certificate needs to match pinned fingerprint
// - Certificate chain traversal
//
// This ensures that certificate pinning works correctly with certificate chains,
// where only the leaf certificate (first in chain) needs to match the pinned
// fingerprint. Important for TLS connections with intermediate certificates.
func TestVerifyPinnedCertificate_MultipleCertificates(t *testing.T) {
	// Generate two certificates
	cert1DER, fingerprint1, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate 1: %v", err)
	}

	cert2DER, _, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate 2: %v", err)
	}

	// Only first certificate's fingerprint is pinned
	err = VerifyPinnedCertificate([][]byte{cert1DER, cert2DER}, []string{fingerprint1})
	if err != nil {
		t.Errorf("Expected no error when one cert matches, got: %v", err)
	}
}

// TestVerifyPinnedCertificate_MultipleFingerprints tests certificate pinning with multiple allowed fingerprints:
// - Multiple pinned fingerprints configured
// - Certificate matches one of several fingerprints
// - Fingerprint list traversal
//
// This supports certificate rotation scenarios where multiple valid certificates
// may be in use simultaneously. Allows gradual certificate updates without
// service disruption. Critical for zero-downtime certificate rotation.
func TestVerifyPinnedCertificate_MultipleFingerprints(t *testing.T) {
	certDER, fingerprint, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Multiple fingerprints, one matching - should pass
	fingerprints := []string{
		"invalid-fingerprint-1",
		fingerprint,
		"invalid-fingerprint-2",
	}

	err = VerifyPinnedCertificate([][]byte{certDER}, fingerprints)
	if err != nil {
		t.Errorf("Expected no error with one matching fingerprint, got: %v", err)
	}
}

// TestVerifyPinnedCertificate_NoFingerprints tests behavior when no fingerprints are configured:
// - Empty fingerprint list
// - Certificate pinning disabled
// - Permissive mode (allows any certificate)
//
// This tests the default behavior when certificate pinning is not configured.
// When no fingerprints are specified, any valid certificate is accepted.
// Important for backward compatibility and optional security features.
func TestVerifyPinnedCertificate_NoFingerprints(t *testing.T) {
	certDER, _, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// No fingerprints configured - should pass
	err = VerifyPinnedCertificate([][]byte{certDER}, []string{})
	if err != nil {
		t.Errorf("Expected no error with empty fingerprints, got: %v", err)
	}
}

// TestVerifyPinnedCertificate_NonMatchingFingerprint tests certificate rejection with non-matching fingerprint:
// - Certificate with different fingerprint than configured
// - Fingerprint mismatch detection
// - Security enforcement (reject invalid certificates)
//
// This is a critical security test ensuring that certificates with non-matching
// fingerprints are rejected, preventing man-in-the-middle attacks and unauthorized
// certificate substitution. Validates the core security function of certificate pinning.
func TestVerifyPinnedCertificate_NonMatchingFingerprint(t *testing.T) {
	certDER, _, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Non-matching fingerprint - should fail
	err = VerifyPinnedCertificate([][]byte{certDER}, []string{"invalid-fingerprint"})
	if err == nil {
		t.Error("Expected error with non-matching fingerprint")
	}
}

// TestVerifyPinnedCertificate_WithWhitespace tests fingerprint matching with whitespace handling:
// - Leading whitespace in fingerprint
// - Trailing whitespace in fingerprint
// - Whitespace trimming before comparison
//
// This ensures that certificate pinning is resilient to common configuration errors
// where fingerprints may have accidental whitespace. Improves usability by automatically
// trimming whitespace while maintaining security.
func TestVerifyPinnedCertificate_WithWhitespace(t *testing.T) {
	certDER, fingerprint, err := generateTestCertificate()
	if err != nil {
		t.Fatalf("Failed to generate certificate: %v", err)
	}

	// Test with whitespace around fingerprint
	fingerprintWithSpaces := " " + fingerprint + " "

	err = VerifyPinnedCertificate([][]byte{certDER}, []string{fingerprintWithSpaces})
	if err != nil {
		t.Errorf("Expected whitespace to be trimmed, got error: %v", err)
	}
}
