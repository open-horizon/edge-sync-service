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

func TestVerifyPinnedCertificate_InvalidCertificate(t *testing.T) {
	// Invalid certificate data
	invalidCert := []byte("not-a-certificate")

	err := VerifyPinnedCertificate([][]byte{invalidCert}, []string{"some-fingerprint"})
	if err == nil {
		t.Error("Expected error with invalid certificate")
	}
}

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
