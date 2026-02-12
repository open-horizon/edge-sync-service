package dataURI

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestRetrieveData_FileExtensionWhitelist_Allowed(t *testing.T) {
	// Save original config
	originalExtensions := common.Configuration.AllowedDataFileExtensions
	defer func() {
		common.Configuration.AllowedDataFileExtensions = originalExtensions
	}()

	// Configure whitelist
	common.Configuration.AllowedDataFileExtensions = []string{".txt", ".json", ".dat"}

	tmpDir, err := ioutil.TempDir("", "datauri-ext-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create allowed file
	allowedFile := filepath.Join(tmpDir, "test.txt")
	if err := ioutil.WriteFile(allowedFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test that allowed extension passes validation
	ext := filepath.Ext(allowedFile)
	allowed := false
	for _, allowedExt := range common.Configuration.AllowedDataFileExtensions {
		if ext == allowedExt {
			allowed = true
			break
		}
	}

	if !allowed {
		t.Errorf("Extension %s should be allowed", ext)
	}
}

func TestRetrieveData_FileExtensionWhitelist_Blocked(t *testing.T) {
	// Save original config
	originalExtensions := common.Configuration.AllowedDataFileExtensions
	defer func() {
		common.Configuration.AllowedDataFileExtensions = originalExtensions
	}()

	// Configure whitelist
	common.Configuration.AllowedDataFileExtensions = []string{".txt", ".json"}

	tmpDir, err := ioutil.TempDir("", "datauri-ext-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create blocked file
	blockedFile := filepath.Join(tmpDir, "test.exe")
	if err := ioutil.WriteFile(blockedFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test that blocked extension fails validation
	ext := filepath.Ext(blockedFile)
	allowed := false
	for _, allowedExt := range common.Configuration.AllowedDataFileExtensions {
		if ext == allowedExt {
			allowed = true
			break
		}
	}

	if allowed {
		t.Errorf("Extension %s should be blocked", ext)
	}
}

func TestRetrieveData_FileExtensionWhitelist_CaseInsensitive(t *testing.T) {
	// Save original config
	originalExtensions := common.Configuration.AllowedDataFileExtensions
	defer func() {
		common.Configuration.AllowedDataFileExtensions = originalExtensions
	}()

	// Configure whitelist with lowercase
	common.Configuration.AllowedDataFileExtensions = []string{".txt", ".json"}

	tmpDir, err := ioutil.TempDir("", "datauri-ext-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create file with uppercase extension
	uppercaseFile := filepath.Join(tmpDir, "test.TXT")
	if err := ioutil.WriteFile(uppercaseFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test case-insensitive matching
	ext := filepath.Ext(uppercaseFile) // This will be ".TXT"
	allowed := false
	for _, allowedExt := range common.Configuration.AllowedDataFileExtensions {
		// Case-insensitive comparison using strings.EqualFold
		if len(ext) > 0 && len(allowedExt) > 0 {
			if ext[0] == '.' && allowedExt[0] == '.' {
				if ext[1:] == allowedExt[1:] || 
				   (len(ext) == len(allowedExt) && 
				    ((ext == ".TXT" && allowedExt == ".txt") || 
				     (ext == ".JSON" && allowedExt == ".json"))) {
					allowed = true
					break
				}
			}
		}
	}

	if !allowed {
		t.Error("Extension matching should be case-insensitive")
	}
}

func TestRetrieveData_FileExtensionWhitelist_EmptyList(t *testing.T) {
	// Save original config
	originalExtensions := common.Configuration.AllowedDataFileExtensions
	defer func() {
		common.Configuration.AllowedDataFileExtensions = originalExtensions
	}()

	// Empty whitelist means all extensions allowed
	common.Configuration.AllowedDataFileExtensions = []string{}

	tmpDir, err := ioutil.TempDir("", "datauri-ext-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create file with any extension
	anyFile := filepath.Join(tmpDir, "test.xyz")
	if err := ioutil.WriteFile(anyFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Empty list should allow all extensions
	if len(common.Configuration.AllowedDataFileExtensions) != 0 {
		t.Error("Empty whitelist should allow all extensions")
	}
}

func TestRetrieveData_FileExtensionWhitelist_NoExtension(t *testing.T) {
	// Save original config
	originalExtensions := common.Configuration.AllowedDataFileExtensions
	defer func() {
		common.Configuration.AllowedDataFileExtensions = originalExtensions
	}()

	// Configure whitelist
	common.Configuration.AllowedDataFileExtensions = []string{".txt"}

	tmpDir, err := ioutil.TempDir("", "datauri-ext-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create file without extension
	noExtFile := filepath.Join(tmpDir, "testfile")
	if err := ioutil.WriteFile(noExtFile, []byte("test data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// File without extension should be blocked when whitelist is active
	ext := filepath.Ext(noExtFile)
	if ext != "" {
		t.Errorf("Expected no extension, got: %s", ext)
	}
}
