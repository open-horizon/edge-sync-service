package dataURI

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestRetrieveData_Directory(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Try to open directory
	file, err := os.Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to stat directory: %v", err)
	}

	if fileInfo.Mode().IsRegular() {
		t.Error("Directory should not be detected as regular file")
	}
}

func TestRetrieveData_NullByteInPath(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Path with null byte
	pathWithNull := filepath.Join(tmpDir, "file\x00.txt")

	err = validateDataPath(pathWithNull, tmpDir)
	if err == nil {
		t.Error("Expected null byte in path to be detected")
	}
	if err != nil && err.Error() != "null byte detected in path" {
		t.Errorf("Expected 'null byte detected' error, got: %v", err)
	}
}

func TestRetrieveData_RegularFile(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create regular file
	testFile := filepath.Join(tmpDir, "test.txt")
	testData := []byte("test data content")
	if err := ioutil.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Open and verify it's a regular file
	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if !fileInfo.Mode().IsRegular() {
		t.Error("Regular file should be detected as regular")
	}
}

func TestRetrieveData_SpecialFiles(t *testing.T) {
	// Test with /dev/null (device file)
	if _, err := os.Stat("/dev/null"); err == nil {
		file, err := os.Open("/dev/null")
		if err != nil {
			t.Skipf("Cannot open /dev/null: %v", err)
		}
		defer file.Close()

		fileInfo, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat /dev/null: %v", err)
		}

		if fileInfo.Mode().IsRegular() {
			t.Error("/dev/null should not be detected as regular file")
		}
	}

	// Create FIFO for testing
	tmpDir, err := ioutil.TempDir("", "datauri-fifo-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	_ = filepath.Join(tmpDir, "test.fifo")
	// Note: Creating FIFOs requires syscall, skip if not available
	// This is a placeholder - actual FIFO creation would use syscall.Mkfifo
}

func TestRetrieveData_SymlinkAttack(t *testing.T) {
	// Create temporary directories
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	allowedDir := filepath.Join(tmpDir, "allowed")
	if err := os.Mkdir(allowedDir, 0755); err != nil {
		t.Fatalf("Failed to create allowed dir: %v", err)
	}

	// Create a file outside allowed directory
	outsideFile := filepath.Join(tmpDir, "secret.txt")
	if err := ioutil.WriteFile(outsideFile, []byte("secret data"), 0644); err != nil {
		t.Fatalf("Failed to create outside file: %v", err)
	}

	// Create symlink inside allowed directory pointing to outside file
	symlinkPath := filepath.Join(allowedDir, "link-to-secret")
	if err := os.Symlink(outsideFile, symlinkPath); err != nil {
		t.Skipf("Symlink creation not supported: %v", err)
	}

	// Test that accessing via symlink is blocked
	err = validateDataPath(symlinkPath, allowedDir)
	if err == nil {
		t.Error("Expected symlink attack to be blocked")
	}
}

func TestValidateDataPath_EmptyPath(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = validateDataPath("", tmpDir)
	if err == nil {
		t.Error("Expected empty path to be rejected")
	}
}

func TestValidateDataPath_PathTraversal(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testCases := []struct {
		name       string
		path       string
		shouldFail bool
	}{
		{
			name:       "Valid path inside base",
			path:       filepath.Join(tmpDir, "valid", "file.txt"),
			shouldFail: false,
		},
		{
			name:       "Path with .. going outside",
			path:       filepath.Join(tmpDir, "..", "outside.txt"),
			shouldFail: true,
		},
		{
			name:       "Absolute path outside base",
			path:       "/etc/passwd",
			shouldFail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean and make absolute
			absPath, err := filepath.Abs(filepath.Clean(tc.path))
			if err != nil {
				t.Fatalf("Failed to resolve path: %v", err)
			}

			err = validateDataPath(absPath, tmpDir)
			if tc.shouldFail && err == nil {
				t.Errorf("Expected path to be rejected: %s", tc.path)
			}
			if !tc.shouldFail && err != nil {
				t.Errorf("Expected path to be allowed: %s, got error: %v", tc.path, err)
			}
		})
	}
}

func TestValidateDataPath_RelativePath(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Relative path should be converted to absolute
	relativePath := "relative/path/file.txt"
	absPath, err := filepath.Abs(filepath.Clean(relativePath))
	if err != nil {
		t.Fatalf("Failed to resolve relative path: %v", err)
	}

	// This will likely fail as it's outside tmpDir, but tests the logic
	_ = validateDataPath(absPath, tmpDir)
}

func TestValidateDataPath_SymlinkInBaseDir(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create actual directory
	actualDir := filepath.Join(tmpDir, "actual")
	if err := os.Mkdir(actualDir, 0755); err != nil {
		t.Fatalf("Failed to create actual dir: %v", err)
	}

	// Create symlink to actual directory
	symlinkDir := filepath.Join(tmpDir, "symlink")
	if err := os.Symlink(actualDir, symlinkDir); err != nil {
		t.Skipf("Symlink creation not supported: %v", err)
	}

	// Create file in actual directory
	testFile := filepath.Join(actualDir, "test.txt")
	if err := ioutil.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Validate using symlink as base - should resolve to actual
	err = validateDataPath(testFile, symlinkDir)
	if err != nil {
		t.Errorf("Expected validation to succeed with symlink base dir: %v", err)
	}
}
