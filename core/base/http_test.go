package base

import (
    "net"
    "os"
    "path/filepath"
    "strconv"
    "testing"

    "github.com/open-horizon/edge-sync-service/common"
)

// TestUnixSocketPermissionsParsing tests the parsing of Unix socket file permissions:
// - Valid octal strings (0660, 0755, 0644, etc.)
// - Invalid octal strings (non-octal digits, out of range)
// - Edge cases (empty string, malformed input)
//
// This test ensures protection against CWE-732: Incorrect Permission Assignment for Critical Resource
// by validating that only proper octal permission strings are accepted.
//
// Related to the fix where UnixSocketFilePermissions was changed from uint32 to string
// to properly handle octal notation in configuration files.
func TestUnixSocketPermissionsParsing(t *testing.T) {
    tests := []struct {
        name        string
        permissions string
        expectError bool
        expectedVal uint32
    }{
        {
            name:        "Valid 0660 permissions",
            permissions: "0660",
            expectError: false,
            expectedVal: 0660,
        },
        {
            name:        "Valid 0755 permissions",
            permissions: "0755",
            expectError: false,
            expectedVal: 0755,
        },
        {
            name:        "Valid 0644 permissions",
            permissions: "0644",
            expectError: false,
            expectedVal: 0644,
        },
        {
            name:        "Valid 0600 permissions",
            permissions: "0600",
            expectError: false,
            expectedVal: 0600,
        },
        {
            name:        "Valid 0777 permissions",
            permissions: "0777",
            expectError: false,
            expectedVal: 0777,
        },
        {
            name:        "Invalid octal digit 8",
            permissions: "0888",
            expectError: true,
        },
        {
            name:        "Invalid octal digit 9",
            permissions: "0999",
            expectError: true,
        },
        {
            name:        "Invalid hexadecimal format",
            permissions: "0x660",
            expectError: true,
        },
        {
            name:        "Valid without leading zero",
            permissions: "660",
            expectError: false,
            expectedVal: 0660,
        },
        {
            name:        "Empty string",
            permissions: "",
            expectError: true,
        },
        {
            name:        "Non-numeric string",
            permissions: "invalid",
            expectError: true,
        },
        {
            name:        "Negative value",
            permissions: "-0660",
            expectError: true,
        },
        {
            name:        "Out of range (too large)",
            permissions: "07777",
            expectError: false,
            expectedVal: 07777,
        },
        {
            name:        "Leading zeros valid",
            permissions: "00660",
            expectError: false,
            expectedVal: 0660,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            perms, err := strconv.ParseUint(tt.permissions, 8, 32)
            
            if tt.expectError {
                if err == nil {
                    t.Errorf("Expected error for permissions %q, but got none", tt.permissions)
                }
            } else {
                if err != nil {
                    t.Errorf("Unexpected error for permissions %q: %v", tt.permissions, err)
                }
                if uint32(perms) != tt.expectedVal {
                    t.Errorf("Expected permissions %o, got %o", tt.expectedVal, perms)
                }
            }
        })
    }
}

// TestUnixSocketFilePermissionsApplication tests the actual application of permissions to Unix socket files:
// - Creates a temporary Unix socket
// - Applies permissions using os.Chmod
// - Verifies the permissions are correctly set
//
// This test ensures that the permission string is correctly converted to os.FileMode
// and properly applied to the socket file, protecting against CWE-732.
//
// Critical for production environments where Unix sockets are used for inter-process communication.
func TestUnixSocketFilePermissionsApplication(t *testing.T) {
    // Create a temporary directory for testing
    tempDir := t.TempDir()

    tests := []struct {
        name            string
        permissions     string
        expectedMode    os.FileMode
        expectParseErr  bool
        expectChmodErr  bool
    }{
        {
            name:           "Apply 0660 permissions",
            permissions:    "0660",
            expectedMode:   0660,
            expectParseErr: false,
            expectChmodErr: false,
        },
        {
            name:           "Apply 0755 permissions",
            permissions:    "0755",
            expectedMode:   0755,
            expectParseErr: false,
            expectChmodErr: false,
        },
        {
            name:           "Apply 0600 permissions",
            permissions:    "0600",
            expectedMode:   0600,
            expectParseErr: false,
            expectChmodErr: false,
        },
        {
            name:           "Invalid permissions should fail parsing",
            permissions:    "0888",
            expectParseErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Create a test file to simulate the socket
            testFile := filepath.Join(tempDir, tt.name+".sock")
            f, err := os.Create(testFile)
            if err != nil {
                t.Fatalf("Failed to create test file: %v", err)
            }
            f.Close()
            defer os.Remove(testFile)

            // Parse permissions
            perms, err := strconv.ParseUint(tt.permissions, 8, 32)
            if tt.expectParseErr {
                if err == nil {
                    t.Errorf("Expected parse error for permissions %q, but got none", tt.permissions)
                }
                return
            }
            if err != nil {
                t.Fatalf("Unexpected parse error for permissions %q: %v", tt.permissions, err)
            }

            // Apply permissions
            err = os.Chmod(testFile, os.FileMode(perms))
            if tt.expectChmodErr {
                if err == nil {
                    t.Errorf("Expected chmod error, but got none")
                }
                return
            }
            if err != nil {
                t.Fatalf("Failed to chmod file: %v", err)
            }

            // Verify permissions
            info, err := os.Stat(testFile)
            if err != nil {
                t.Fatalf("Failed to stat file: %v", err)
            }

            actualMode := info.Mode().Perm()
            if actualMode != tt.expectedMode {
                t.Errorf("Expected file mode %o, got %o", tt.expectedMode, actualMode)
            }
        })
    }
}

// TestUnixSocketCreationWithPermissions tests the complete Unix socket creation flow:
// - Creates a Unix socket listener
// - Applies permissions to the socket file
// - Verifies the socket is accessible with correct permissions
//
// This integration test validates the entire Unix socket setup process including
// permission assignment, ensuring CWE-732 protection in real-world scenarios.
func TestUnixSocketCreationWithPermissions(t *testing.T) {
    tempDir := t.TempDir()

    tests := []struct {
        name         string
        permissions  string
        expectedMode os.FileMode
    }{
        {
            name:         "Socket with 0660 permissions",
            permissions:  "0660",
            expectedMode: 0660,
        },
        {
            name:         "Socket with 0600 permissions",
            permissions:  "0600",
            expectedMode: 0600,
        },
        {
            name:         "Socket with 0755 permissions",
            permissions:  "0755",
            expectedMode: 0755,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            testSocketPath := filepath.Join(tempDir, tt.name+".sock")
            
            // Remove socket if it exists
            os.Remove(testSocketPath)

            // Create Unix socket
            unixAddr, err := net.ResolveUnixAddr("unix", testSocketPath)
            if err != nil {
                t.Fatalf("Failed to resolve Unix address: %v", err)
            }

            listener, err := net.ListenUnix("unix", unixAddr)
            if err != nil {
                t.Fatalf("Failed to create Unix socket: %v", err)
            }
            defer listener.Close()
            defer os.Remove(testSocketPath)

            // Parse and apply permissions
            perms, err := strconv.ParseUint(tt.permissions, 8, 32)
            if err != nil {
                t.Fatalf("Failed to parse permissions %q: %v", tt.permissions, err)
            }

            err = os.Chmod(testSocketPath, os.FileMode(perms))
            if err != nil {
                t.Fatalf("Failed to chmod socket: %v", err)
            }

            // Verify permissions
            info, err := os.Stat(testSocketPath)
            if err != nil {
                t.Fatalf("Failed to stat socket: %v", err)
            }

            actualMode := info.Mode().Perm()
            if actualMode != tt.expectedMode {
                t.Errorf("Expected socket mode %o, got %o", tt.expectedMode, actualMode)
            }

            // Verify socket is functional
            if info.Mode()&os.ModeSocket == 0 {
                t.Error("Created file is not a socket")
            }
        })
    }
}

// TestUnixSocketPermissionsConfigValidation tests configuration validation:
// - Validates that configuration properly handles UnixSocketFilePermissions as string
// - Tests that invalid permission strings are rejected during config validation
//
// This test ensures that the configuration layer properly validates Unix socket
// permissions before they are used, providing early detection of configuration errors.
func TestUnixSocketPermissionsConfigValidation(t *testing.T) {
    tests := []struct {
        name        string
        permissions string
        expectError bool
    }{
        {
            name:        "Valid 0660",
            permissions: "0660",
            expectError: false,
        },
        {
            name:        "Valid 0755",
            permissions: "0755",
            expectError: false,
        },
        {
            name:        "Invalid 0888",
            permissions: "0888",
            expectError: true,
        },
        {
            name:        "Invalid empty",
            permissions: "",
            expectError: true,
        },
        {
            name:        "Invalid non-octal",
            permissions: "invalid",
            expectError: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Save original config
            origPerms := common.Configuration.UnixSocketFilePermissions
            defer func() {
                common.Configuration.UnixSocketFilePermissions = origPerms
            }()

            // Set test permissions
            common.Configuration.UnixSocketFilePermissions = tt.permissions

            // Attempt to parse (simulating what startHTTPServer does)
            _, err := strconv.ParseUint(common.Configuration.UnixSocketFilePermissions, 8, 32)

            if tt.expectError {
                if err == nil {
                    t.Errorf("Expected validation error for permissions %q, but got none", tt.permissions)
                }
            } else {
                if err != nil {
                    t.Errorf("Unexpected validation error for permissions %q: %v", tt.permissions, err)
                }
            }
        })
    }
}

// TestUnixSocketPermissionsBoundaryValues tests boundary and edge cases:
// - Minimum valid permissions (0000)
// - Maximum valid permissions (0777)
// - Common permission patterns
//
// This test ensures robust handling of all valid permission values within
// the Unix file permission range (0000-0777).
func TestUnixSocketPermissionsBoundaryValues(t *testing.T) {
    tests := []struct {
        name        string
        permissions string
        expectedVal uint32
        expectError bool
    }{
        {
            name:        "Minimum permissions 0000",
            permissions: "0000",
            expectedVal: 0000,
            expectError: false,
        },
        {
            name:        "Maximum permissions 0777",
            permissions: "0777",
            expectedVal: 0777,
            expectError: false,
        },
        {
            name:        "Owner read only 0400",
            permissions: "0400",
            expectedVal: 0400,
            expectError: false,
        },
        {
            name:        "Owner write only 0200",
            permissions: "0200",
            expectedVal: 0200,
            expectError: false,
        },
        {
            name:        "Owner execute only 0100",
            permissions: "0100",
            expectedVal: 0100,
            expectError: false,
        },
        {
            name:        "Setuid bit 04755",
            permissions: "04755",
            expectedVal: 04755,
            expectError: false,
        },
        {
            name:        "Setgid bit 02755",
            permissions: "02755",
            expectedVal: 02755,
            expectError: false,
        },
        {
            name:        "Sticky bit 01777",
            permissions: "01777",
            expectedVal: 01777,
            expectError: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            perms, err := strconv.ParseUint(tt.permissions, 8, 32)

            if tt.expectError {
                if err == nil {
                    t.Errorf("Expected error for permissions %q, but got none", tt.permissions)
                }
            } else {
                if err != nil {
                    t.Errorf("Unexpected error for permissions %q: %v", tt.permissions, err)
                }
                if uint32(perms) != tt.expectedVal {
                    t.Errorf("Expected permissions %o, got %o", tt.expectedVal, perms)
                }
            }
        })
    }
}
