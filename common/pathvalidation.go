package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidateFilePath validates a file path against security requirements.
// It performs the following checks:
// - Cleans the path to remove redundant separators and resolve . and .. elements
// - Checks for null bytes (CWE-158)
// - Resolves symlinks to prevent symlink attacks (CWE-61)
// - Ensures the path is within the allowed base directory (CWE-22)
//
// Returns the validated absolute path or an error if validation fails.
func ValidateFilePath(path, baseDir string) (string, error) {
	// Clean the path to normalize it
	cleanPath := filepath.Clean(path)

	// Check for null bytes (CWE-158: Improper Neutralization of Null Byte)
	if strings.Contains(cleanPath, "\x00") {
		return "", fmt.Errorf("invalid file path: path contains null byte")
	}

	// Resolve symlinks to prevent symlink attacks (CWE-61)
	resolvedPath, err := filepath.EvalSymlinks(cleanPath)
	if err != nil {
		// If symlink resolution fails (e.g., file doesn't exist yet), use clean path
		// This allows validation of paths for files that will be created
		resolvedPath = cleanPath
	}

	// Get absolute paths for comparison
	absPath, err := filepath.Abs(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("invalid file path: failed to get absolute path: %w", err)
	}

	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("invalid file path: failed to get absolute base directory: %w", err)
	}

	// Ensure the path is within the base directory (CWE-22: Path Traversal)
	// The path must either be exactly the base directory or start with base directory + separator
	if absPath != absBase && !strings.HasPrefix(absPath, absBase+string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid file path: path traversal detected: %s is outside allowed directory %s", absPath, absBase)
	}

	return absPath, nil
}

// ValidateFilePathWithExtension validates a file path and checks if the file extension
// is in the allowed list. Extension matching is case-insensitive.
//
// If allowedExtensions is empty, no extension checking is performed.
// Extensions should include the leading dot (e.g., ".pem", ".crt", ".cert").
//
// Returns the validated absolute path or an error if validation fails.
func ValidateFilePathWithExtension(path, baseDir string, allowedExtensions []string) (string, error) {
	// First validate the path
	validPath, err := ValidateFilePath(path, baseDir)
	if err != nil {
		return "", err
	}

	// If no extensions specified, skip extension check
	if len(allowedExtensions) == 0 {
		return validPath, nil
	}

	// Check if file extension is allowed (case-insensitive)
	ext := strings.ToLower(filepath.Ext(validPath))
	allowed := false
	for _, allowedExt := range allowedExtensions {
		if strings.ToLower(allowedExt) == ext {
			allowed = true
			break
		}
	}

	if !allowed {
		return "", fmt.Errorf("invalid file path: file extension %s not allowed", ext)
	}

	return validPath, nil
}
