package common

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateFilePath_ValidPaths(t *testing.T) {
	baseDir := "/var/edge-sync-service"

	tests := []struct {
		name string
		path string
	}{
		{"simple file", filepath.Join(baseDir, "config.txt")},
		{"nested file", filepath.Join(baseDir, "subdir", "file.txt")},
		{"base directory itself", baseDir},
		{"file with dots in name", filepath.Join(baseDir, "my.config.file.txt")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if err != nil {
				t.Errorf("ValidateFilePath() unexpected error for valid path: %v", err)
			}
		})
	}
}

func TestValidateFilePath_PathTraversal(t *testing.T) {
	baseDir := "/var/edge-sync-service"

	tests := []struct {
		name string
		path string
	}{
		{"path traversal up three levels", filepath.Join(baseDir, "../../../etc/passwd")},
		{"path traversal up two levels", filepath.Join(baseDir, "../../etc/passwd")},
		{"path traversal up one level", filepath.Join(baseDir, "../other-dir/file.txt")},
		{"absolute path outside", "/etc/passwd"},
		{"absolute path to root", "/"},
		{"path with multiple ..", filepath.Join(baseDir, "subdir", "..", "..", "outside.txt")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if err == nil {
				t.Errorf("ValidateFilePath() expected error for path traversal attempt: %s", tt.path)
			}
		})
	}
}

func TestValidateFilePath_NullByteInjection(t *testing.T) {
	baseDir := "/var/edge-sync-service"

	tests := []struct {
		name string
		path string
	}{
		{"null byte at end", filepath.Join(baseDir, "file.txt\x00")},
		{"null byte in middle", filepath.Join(baseDir, "file\x00.txt")},
		{"null byte at start", filepath.Join(baseDir, "\x00file.txt")},
		{"multiple null bytes", filepath.Join(baseDir, "fi\x00le\x00.txt")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if err == nil {
				t.Errorf("ValidateFilePath() expected error for null byte injection: %s", tt.name)
			}
			if err != nil && !contains(err.Error(), "null byte") {
				t.Errorf("ValidateFilePath() error should mention null byte, got: %v", err)
			}
		})
	}
}

func TestValidateFilePath_SymlinkAttack(t *testing.T) {
	// Create temporary directories for testing
	baseDir, err := os.MkdirTemp("", "base-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	outsideDir, err := os.MkdirTemp("", "outside-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outsideDir)

	// Create a file in the outside directory
	outsideFile := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("secret data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create symlink pointing outside base directory
	symlinkPath := filepath.Join(baseDir, "malicious_link")
	if err := os.Symlink(outsideDir, symlinkPath); err != nil {
		t.Skip("Symlink creation not supported on this system")
	}

	// Should detect symlink attack
	_, err = ValidateFilePath(symlinkPath, baseDir)
	if err == nil {
		t.Error("Expected symlink attack to be blocked")
	}

	// Try to access file through symlink
	symlinkFile := filepath.Join(symlinkPath, "secret.txt")
	_, err = ValidateFilePath(symlinkFile, baseDir)
	if err == nil {
		t.Error("Expected symlink attack via file access to be blocked")
	}
}

func TestValidateFilePath_RelativePaths(t *testing.T) {
	// Create temporary directory for testing
	baseDir, err := os.MkdirTemp("", "base-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	// Change to base directory
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)

	if err := os.Chdir(baseDir); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"relative path within base", "subdir/file.txt", false},
		{"relative path with dot", "./file.txt", false},
		{"relative path traversal", "../outside.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateFilePathWithExtension_ValidExtensions(t *testing.T) {
	baseDir := "/var/edge-sync-service"
	allowedExts := []string{".pem", ".crt", ".cert", ".key"}

	tests := []struct {
		name string
		path string
	}{
		{"pem extension", filepath.Join(baseDir, "cert.pem")},
		{"crt extension", filepath.Join(baseDir, "cert.crt")},
		{"cert extension", filepath.Join(baseDir, "cert.cert")},
		{"key extension", filepath.Join(baseDir, "private.key")},
		{"uppercase PEM", filepath.Join(baseDir, "cert.PEM")},
		{"mixed case CrT", filepath.Join(baseDir, "cert.CrT")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePathWithExtension(tt.path, baseDir, allowedExts)
			if err != nil {
				t.Errorf("ValidateFilePathWithExtension() unexpected error for valid extension: %v", err)
			}
		})
	}
}

func TestValidateFilePathWithExtension_InvalidExtensions(t *testing.T) {
	baseDir := "/var/edge-sync-service"
	allowedExts := []string{".pem", ".crt", ".cert"}

	tests := []struct {
		name string
		path string
	}{
		{"txt extension", filepath.Join(baseDir, "file.txt")},
		{"exe extension", filepath.Join(baseDir, "malware.exe")},
		{"no extension", filepath.Join(baseDir, "file")},
		{"wrong extension", filepath.Join(baseDir, "cert.key")},
		{"double extension", filepath.Join(baseDir, "file.txt.pem")}, // Should pass (last ext is .pem)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePathWithExtension(tt.path, baseDir, allowedExts)
			// Double extension case should pass
			if tt.name == "double extension" {
				if err != nil {
					t.Errorf("ValidateFilePathWithExtension() unexpected error for double extension: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("ValidateFilePathWithExtension() expected error for invalid extension: %s", tt.path)
				}
			}
		})
	}
}

func TestValidateFilePathWithExtension_EmptyAllowedList(t *testing.T) {
	baseDir := "/var/edge-sync-service"
	emptyExts := []string{}

	tests := []struct {
		name string
		path string
	}{
		{"any extension allowed", filepath.Join(baseDir, "file.txt")},
		{"no extension allowed", filepath.Join(baseDir, "file")},
		{"exe allowed", filepath.Join(baseDir, "program.exe")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePathWithExtension(tt.path, baseDir, emptyExts)
			if err != nil {
				t.Errorf("ValidateFilePathWithExtension() unexpected error with empty allowed list: %v", err)
			}
		})
	}
}

func TestValidateFilePathWithExtension_PathTraversalWithValidExtension(t *testing.T) {
	baseDir := "/var/edge-sync-service"
	allowedExts := []string{".pem"}

	// Even with valid extension, path traversal should be blocked
	path := filepath.Join(baseDir, "../../../etc/ssl/cert.pem")
	_, err := ValidateFilePathWithExtension(path, baseDir, allowedExts)
	if err == nil {
		t.Error("ValidateFilePathWithExtension() should block path traversal even with valid extension")
	}
}

func TestValidateFilePath_WindowsPaths(t *testing.T) {
	if filepath.Separator != '\\' {
		t.Skip("Windows-specific test")
	}

	baseDir := `C:\edge-sync-service`

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"valid windows path", filepath.Join(baseDir, "config.txt"), false},
		{"windows path traversal", filepath.Join(baseDir, `..\..\Windows\System32\config`), true},
		{"absolute windows path", `C:\Windows\System32\config`, true},
		{"UNC path", `\\server\share\file.txt`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestValidateFilePath_SpecialCharacters tests path validation with special characters including:
// - Unicode characters (Cyrillic, Chinese, etc.)
// - Spaces in filenames
// - Special characters (@, #, $)
// - Emoji characters
// - Multiple dots in filenames
// - Hyphens and underscores
// This ensures the path validation doesn't reject legitimate filenames with non-ASCII characters.
func TestValidateFilePath_SpecialCharacters(t *testing.T) {
	baseDir := t.TempDir()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"unicode filename", filepath.Join(baseDir, "файл.txt"), false},
		{"spaces in filename", filepath.Join(baseDir, "file name.txt"), false},
		{"special chars", filepath.Join(baseDir, "file@#$.txt"), false},
		{"emoji in filename", filepath.Join(baseDir, "file😀.txt"), false},
		{"dots in filename", filepath.Join(baseDir, "my.config.file.txt"), false},
		{"hyphen and underscore", filepath.Join(baseDir, "my-file_name.txt"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateFilePath(tt.path, baseDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateFilePath_FilesystemLimits tests path validation with filesystem boundary conditions:
// - Long filenames (200+ characters)
// - Deeply nested directory structures (20+ levels)
// - Paths with many directory components (50+ directories)
// This ensures the validation handles edge cases near filesystem limits without failing
// on legitimate but unusual path structures.
func TestValidateFilePath_FilesystemLimits(t *testing.T) {
	baseDir := t.TempDir()

	tests := []struct {
		name    string
		pathGen func() string
		wantErr bool
	}{
		{
			name: "long filename (200 chars)",
			pathGen: func() string {
				longName := strings.Repeat("a", 200) + ".txt"
				return filepath.Join(baseDir, longName)
			},
			wantErr: false,
		},
		{
			name: "deeply nested path (20 levels)",
			pathGen: func() string {
				path := baseDir
				for i := 0; i < 20; i++ {
					path = filepath.Join(path, fmt.Sprintf("dir%d", i))
				}
				return filepath.Join(path, "file.txt")
			},
			wantErr: false,
		},
		{
			name: "path with many components",
			pathGen: func() string {
				path := baseDir
				for i := 0; i < 50; i++ {
					path = filepath.Join(path, "d")
				}
				return filepath.Join(path, "file.txt")
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.pathGen()
			_, err := ValidateFilePath(path, baseDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateFilePathWithExtension_ConfigurationDriven tests extension validation using configuration parameters:
// - Configuration.AllowedCertificateExtensions for certificate files
// - Configuration.AllowedKeyExtensions for private key files
// - Empty configuration (should allow all extensions)
// - Custom extensions from environment variables
// This ensures the centralized configuration approach works correctly and allows
// deployment-specific customization of allowed file extensions.
func TestValidateFilePathWithExtension_ConfigurationDriven(t *testing.T) {
	baseDir := t.TempDir()

	// Save original configuration
	origCertExts := Configuration.AllowedCertificateExtensions
	origKeyExts := Configuration.AllowedKeyExtensions
	defer func() {
		Configuration.AllowedCertificateExtensions = origCertExts
		Configuration.AllowedKeyExtensions = origKeyExts
	}()

	tests := []struct {
		name       string
		setupFunc  func()
		path       string
		useConfig  bool
		wantErr    bool
	}{
		{
			name: "use configured cert extensions",
			setupFunc: func() {
				Configuration.AllowedCertificateExtensions = []string{".pem", ".crt"}
			},
			path:      filepath.Join(baseDir, "cert.pem"),
			useConfig: true,
			wantErr:   false,
		},
		{
			name: "reject non-configured extension",
			setupFunc: func() {
				Configuration.AllowedCertificateExtensions = []string{".pem"}
			},
			path:      filepath.Join(baseDir, "cert.crt"),
			useConfig: true,
			wantErr:   true,
		},
		{
			name: "empty configuration uses defaults",
			setupFunc: func() {
				Configuration.AllowedCertificateExtensions = []string{}
			},
			path:      filepath.Join(baseDir, "cert.txt"),
			useConfig: true,
			wantErr:   false, // Empty list allows all
		},
		{
			name: "custom key extensions",
			setupFunc: func() {
				Configuration.AllowedKeyExtensions = []string{".key", ".pem"}
			},
			path:      filepath.Join(baseDir, "private.key"),
			useConfig: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupFunc()
			
			var exts []string
			if tt.useConfig {
				if strings.Contains(tt.path, "key") {
					exts = Configuration.AllowedKeyExtensions
				} else {
					exts = Configuration.AllowedCertificateExtensions
				}
			}
			
			_, err := ValidateFilePathWithExtension(tt.path, baseDir, exts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilePathWithExtension() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidateFilePath_ConcurrentAccess tests concurrent path validation to ensure thread safety:
// - 100 goroutines validating the same path simultaneously
// - Verifies no race conditions occur
// - Ensures validation results are consistent across concurrent calls
// This test should be run with the race detector enabled: go test -race
// Critical for production environments with high concurrency.
func TestValidateFilePath_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	
	// Create test file
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	
	const numGoroutines = 100
	errChan := make(chan error, numGoroutines)
	
	// Launch concurrent validations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			_, err := ValidateFilePath(testFile, baseDir)
			errChan <- err
		}()
	}
	
	// Collect results
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent validation failed: %v", err)
		}
	}
}

// TestValidateFilePath_EncodedTraversal tests URL-encoded path traversal attempts:
// - URL-encoded dots (%2e%2e)
// - Double-encoded traversal (%252e%252e)
// - Mixed encoding patterns
// This ensures that encoding tricks cannot bypass path validation security.
// Note: filepath.Join normalizes paths, so encoded characters won't work as traversal,
// but this test documents the expected behavior and ensures no regression.
func TestValidateFilePath_EncodedTraversal(t *testing.T) {
	baseDir := "/var/edge-sync-service"
	
	tests := []struct {
		name string
		path string
	}{
		{"URL encoded dots", filepath.Join(baseDir, "%2e%2e", "etc", "passwd")},
		{"double encoded", filepath.Join(baseDir, "%252e%252e", "etc", "passwd")},
		{"mixed encoding", filepath.Join(baseDir, "..%2f..%2fetc%2fpasswd")},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: filepath.Join normalizes the path, so encoded characters
			// won't work as path traversal. This test documents expected behavior.
			_, err := ValidateFilePath(tt.path, baseDir)
			// The validation should either succeed (if path is within baseDir after normalization)
			// or fail (if it's outside). The key is that encoding doesn't bypass validation.
			if err == nil {
				// If no error, verify the path is actually within baseDir
				validPath, _ := ValidateFilePath(tt.path, baseDir)
				if !strings.HasPrefix(validPath, baseDir) {
					t.Errorf("Encoded path traversal bypassed validation: %s", validPath)
				}
			}
		})
	}
}
