package common

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"strings"
)

// VerifyPinnedCertificate checks if any certificate in the chain matches the pinned fingerprints.
// If no fingerprints are configured (empty slice), this function returns nil to allow standard
// certificate validation to proceed.
//
// Parameters:
//   - rawCerts: Raw certificate data from the TLS handshake
//   - pinnedFingerprints: SHA256 fingerprints (hex-encoded) of certificates to accept
//
// Returns:
//   - nil if a matching certificate is found or no pinning is configured
//   - error if pinning is configured but no matching certificate is found
func VerifyPinnedCertificate(rawCerts [][]byte, pinnedFingerprints []string) error {
	if len(pinnedFingerprints) == 0 {
		return nil // No pinning configured, allow standard validation
	}

	for _, rawCert := range rawCerts {
		cert, err := x509.ParseCertificate(rawCert)
		if err != nil {
			continue
		}

		fingerprint := sha256.Sum256(cert.Raw)
		fingerprintHex := hex.EncodeToString(fingerprint[:])

		for _, pinned := range pinnedFingerprints {
			if strings.EqualFold(strings.TrimSpace(fingerprintHex), strings.TrimSpace(pinned)) {
				return nil // Match found
			}
		}
	}

	return errors.New("certificate does not match any pinned fingerprints")
}
