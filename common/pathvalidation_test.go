package common

import (
	"os"
	"path/filepath"
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
