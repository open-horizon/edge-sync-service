# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

The **Edge Sync Service** is a management tool for Edge Computing that simplifies applications running on the edge by providing tools to synchronize objects between the cloud and edge nodes. It consists of two main components:

1. **Cloud Sync Service (CSS)** - Runs in the cloud, supports multi-tenancy, high availability, and load balancing
2. **Edge Sync Service (ESS)** - Runs on edge nodes, each with a unique Type and ID

### Technology Stack

- **Language**: Go 1.23+
- **Communication Protocols**: MQTT, HTTP/HTTPS, Watson IoT Platform (WIoTP)
- **Storage Backends**: 
  - CSS: MongoDB (default), BoltDB
  - ESS: In-Memory (default), BoltDB
- **Dependencies**: Managed via Go modules (`go.mod`)
- **Container Platform**: Docker (multi-architecture: amd64, armh, arm64, ppc64le, riscv64, s390x)

### Architecture

The service uses a flexible addressing system allowing objects to be sent:
- To a single ESS (using ESS ID)
- To all ESS nodes of a certain Type
- To any group of ESS nodes
- To all ESS nodes

Communication between CSS and ESS can occur over:
- MQTT (direct broker or via Watson IoT Platform)
- HTTP/HTTPS
- Hybrid modes combining both protocols

## Building and Running

### Prerequisites

- Go 1.23 or higher
- Docker (for container builds)
- MongoDB (optional, for CSS with mongo storage)
- MQTT Broker (optional, for MQTT communication)

### Setup

```bash
# Set up Go workspace
export GOPATH=$(pwd)

# Get dependencies
go get -d github.com/open-horizon/edge-sync-service
cd src/github.com/open-horizon/edge-sync-service

# Install dependencies
go mod tidy
go mod vendor
./get_dependencies.sh
```

### Build

**Binary Build:**
```bash
export GOPATH=<workspace-root>
go install github.com/open-horizon/edge-sync-service/cmd/edge-sync-service
```

**Container Build:**
```bash
export GOPATH=<workspace-root>
./buildContainer.sh <platform>  # platform: amd64, armhf, or arm64
```

Container will be tagged as `open-horizon/edge-sync-service:latest`

**Special Requirements for ARM Builds:**
- armhf: Requires `gcc-arm-linux-gnueabihf` and QEMU user-static registration
- arm64: Requires `gcc-aarch64-linux-gnu` and QEMU user-static registration

### Running

**Configuration:**
- Use configuration file (`sync.conf`) or environment variables
- All config parameters have corresponding environment variables (see `sync.conf` for mappings)
- Configuration file location: `/etc/edge-sync-service/sync.conf` (default)

**As Cloud Sync Service (CSS):**
```bash
export NODE_TYPE=CSS
$GOPATH/bin/edge-sync-service [-c <config-file>]
```

**As Edge Sync Service (ESS):**
```bash
export NODE_TYPE=ESS
export DESTINATION_TYPE=<type>
export DESTINATION_ID=<id>
export ORG_ID=<org>
$GOPATH/bin/edge-sync-service [-c <config-file>]
```

### Testing

**Run all tests:**
```bash
go test ./...
```

**Run tests with race detection:**
```bash
go test -race ./...
```

**Note:** Some tests require MongoDB to be running on `localhost:27017`. Tests will fail if MongoDB is unavailable. Use `-short` flag to skip integration tests:
```bash
go test -short ./...
```

**Run coverage:**
```bash
./runCoverage.sh
```

## Development Conventions

### Code Organization

```
cmd/edge-sync-service/     # Main application entry point
common/                    # Shared utilities, configuration, types
core/
  ├── base/               # Core API server and module management
  ├── communications/     # MQTT/HTTP communication handlers
  ├── dataURI/           # Data URI handling and file operations
  ├── dataVerifier/      # Data signature verification
  ├── leader/            # Leadership election for CSS HA
  ├── security/          # Authentication and authorization
  └── storage/           # Storage abstraction (Mongo, Bolt, InMemory)
```

**File and Module Organization Principles:**

1. **Keep Files Manageable**: Aim to keep individual source files under 1000 lines where practical
   - If a file grows beyond 1500 lines, consider splitting it into multiple files or subpackages
   - Large files are harder to navigate, review, and maintain

2. **Leverage Go's Package Structure**: 
   - Create new packages/directories when a set of related functionality becomes substantial
   - Each package should have a clear, single responsibility
   - Use internal packages for implementation details that shouldn't be exposed

3. **When to Create New Modules/Directories**:
   - When a feature or component has multiple related types and functions (>500 lines)
   - When functionality is logically distinct and could be tested independently
   - When code could potentially be reused by other parts of the system
   - When a file has grown too large and splitting within the same package isn't sufficient

4. **File Splitting Strategies**:
   - Split by functionality: `storage.go`, `storage_helpers.go`, `storage_cache.go`
   - Split by type: `mongo_storage.go`, `bolt_storage.go`, `inmemory_storage.go`
   - Split by concern: `api.go`, `api_handlers.go`, `api_validation.go`
   - Keep related tests in corresponding `*_test.go` files

5. **Package Naming**:
   - Use short, descriptive package names (e.g., `storage`, not `storagemanagement`)
   - Avoid generic names like `util`, `common`, `helpers` for new packages
   - Package name should describe what it provides, not what it contains

**Example of Good Module Organization:**
```
core/storage/
  ├── storage.go              # Interface definitions and common types
  ├── storage_test.go         # Common storage tests
  ├── cache.go                # Cache implementation (~300 lines)
  ├── boltStorage.go          # BoltDB implementation (~800 lines)
  ├── boltStorageHelpers.go   # BoltDB helper functions (~400 lines)
  ├── mongoStorage.go         # MongoDB implementation (~900 lines)
  ├── mongoStorageHelpers.go  # MongoDB helper functions (~500 lines)
  └── inMemoryStorage.go      # In-memory implementation (~400 lines)
```

### Source Maintainability Principles

**Avoid Hardcoded Duplication - Favor Centralized Configuration:**

1. **Single Source of Truth**: When the same values, constants, or structures are needed across multiple files or packages, create a centralized, configurable structure rather than duplicating hardcoded values.

2. **Configuration Over Hardcoding**: 
   - **Bad Practice**: Hardcoding `[]string{".pem", ".crt", ".cert"}` in 6 different files
   - **Good Practice**: Define `AllowedCertificateExtensions` once in configuration with defaults, reference everywhere
   - Benefits: Single point of change, consistent behavior, user customization, easier testing

3. **Reusable Structures**:
   - Create shared types, constants, and helper functions in `common/` package
   - Use configuration parameters for values that might need customization
   - Document the centralized structure and its usage in code comments

4. **Real-World Example** (CWE-22 Path Validation):
   ```go
   // Bad: Hardcoded in multiple files
   certExts := []string{".pem", ".crt", ".cert"}  // Repeated 6 times
   
   // Good: Centralized in configuration
   type Config struct {
       AllowedCertificateExtensions []string `env:"ALLOWED_CERTIFICATE_EXTENSIONS"`
   }
   
   // Usage with fallback to defaults
   certExts := common.Configuration.AllowedCertificateExtensions
   if len(certExts) == 0 {
       certExts = []string{".pem", ".crt", ".cert"}
   }
   ```

5. **When to Centralize**:
   - Values used in 3+ locations
   - Security-related constants (file extensions, timeouts, limits)
   - Business logic constants that might change
   - Environment-specific settings
   - Validation rules and constraints

6. **Maintainability Benefits**:
   - **Consistency**: Same behavior across all usages
   - **Flexibility**: Easy to customize per deployment
   - **Testability**: Single place to mock or override for tests
   - **Documentation**: Clear intent and purpose in one location
   - **Evolution**: Easy to extend or modify without hunting through codebase

### Code Style and Organization

**Indentation and Formatting:**
- **Use spaces, not tabs** for indentation
- **Standard indent size: 4 spaces**
- Go's `gofmt` tool enforces tabs by default, but for consistency with other languages and better cross-platform compatibility, convert tabs to spaces in non-Go files
- For Go files, follow `gofmt` conventions (which uses tabs)
- **Trim trailing whitespace** from all lines when editing files
- **End files with a single blank line** (newline at EOF)
- Ensure your editor is configured to:
  - Insert spaces when Tab key is pressed
  - Display tabs as 4 spaces for consistency
  - Convert existing tabs to spaces when editing files
  - Automatically trim trailing whitespace on save
  - Ensure newline at end of file

**Function Ordering:**
- Prefer lexical (alphabetical) ordering of functions within a file where it makes sense
- Exceptions: Group related helper functions near their primary function when it improves comprehension
- Public functions (exported) should generally appear before private functions (unexported)
- Keep `init()` functions at the top of the file after package-level variables

**Lexical Ordering for Other Constructs:**
Apply alphabetical ordering where it makes sense for:
- **Variable declarations**: Group related variables, but within groups prefer alphabetical order
- **Struct fields**: Order fields alphabetically unless logical grouping (e.g., related fields, embedded types first) improves clarity
- **Constants**: Within const blocks, prefer alphabetical ordering
- **Interface methods**: Order methods alphabetically in interface definitions
- **Configuration parameters**: In configuration structs, order fields alphabetically for easier lookup
- **Import statements**: Go automatically formats imports, but group standard library, external, and internal imports

**When NOT to use lexical ordering:**
- When fields have dependencies or initialization order matters
- When grouping by functionality significantly improves comprehension
- When following established patterns in the existing codebase
- When struct field order affects memory layout for performance reasons

**Readability Principles:**
- Prioritize code legibility and readability without sacrificing technical depth or solution quality
- Use clear, descriptive variable and function names that convey intent
- Add comments for complex logic, but prefer self-documenting code
- Break down complex functions into smaller, well-named helper functions
- Use whitespace and formatting to visually separate logical blocks
- Avoid deeply nested code; prefer early returns and guard clauses

**Code Structure:**
- Keep functions focused on a single responsibility
- Limit function length to what fits on a screen when possible (aim for < 50 lines)
- Use consistent error handling patterns throughout the codebase
- Document exported functions, types, and constants with godoc-style comments
- Place package-level constants and variables at the top of files

**Example of Good Organization:**
```go
package example

// Package-level constants
const (
    DefaultTimeout = 30
    MaxRetries     = 3
)

// Package-level variables
var (
    globalCache map[string]interface{}
)

// init function
func init() {
    globalCache = make(map[string]interface{})
}

// Exported functions (alphabetically ordered)
func CreateResource(name string) error { ... }

func DeleteResource(id string) error { ... }

func GetResource(id string) (*Resource, error) { ... }

func UpdateResource(id string, data []byte) error { ... }

// Unexported helper functions (alphabetically ordered)
func buildResourceKey(id string) string { ... }

func validateResourceData(data []byte) error { ... }
```

### Key Design Patterns

1. **Storage Abstraction**: All storage operations go through `storage.Storage` interface
2. **Communication Wrapper**: `communications.Communicator` interface abstracts MQTT/HTTP
3. **Queue-Based Processing**: Object updates processed via work queues (`ObjectWorkQueue`, `ObjectVerifyQueue`)
4. **Leader Election**: CSS instances use leader election for coordinated operations
5. **Security Model**: ACL-based authorization with organization-level isolation

### Multilingual Support Requirements

**REST API Responses:**
- All REST API requests and responses MUST handle multilingual support
- Error messages, status messages, and user-facing text must be localizable
- Use appropriate internationalization (i18n) mechanisms for API responses
- Support language negotiation via Accept-Language headers where applicable
- Ensure consistent message formatting across different languages

**Logging Messages:**
- Logging messages are more permissive and can default to English
- Internal debug/trace logs do not require multilingual support
- Focus multilingual efforts on user-facing API responses and error messages
- Log messages should still be clear and descriptive for debugging purposes

### Dependency Management

**Go Module Updates and Security:**

1. **Regular Dependency Checks**: 
   - Check for dependency updates during any ongoing source code work
   - Use `go list -m -u all` to check for available updates
   - Use `go mod tidy` to clean up unused dependencies

2. **Security Vulnerability Scanning**:
   - Run `go list -json -m all | nancy sleuth` or similar tools to check for CVEs
   - Use GitHub's Dependabot or similar automated scanning tools
   - Prioritize updates that address security vulnerabilities (CVEs)
   - Document CVE fixes in commit messages and pull requests

3. **Update Strategy - Prioritize Stability**:
   - **Critical Security Updates**: Apply immediately after testing
   - **Minor/Patch Updates**: Apply regularly, test thoroughly
   - **Major Version Updates**: Evaluate carefully, may require code changes
   - Always run full test suite after dependency updates: `go test ./...`
   - Always run race detector after updates: `go test -race ./...`
   - Verify builds succeed on all target platforms (amd64, armhf, arm64)

4. **Testing After Updates**:
   ```bash
   # Update dependencies
   go get -u ./...
   go mod tidy
   go mod vendor
   
   # Verify build
   go build ./...
   
   # Run tests
   go test ./...
   go test -race ./...
   
   # Run coverage
   ./runCoverage.sh
   ```

5. **Dependency Update Guidelines**:
   - Never update dependencies without running the full test suite
   - Document breaking changes in dependency updates
   - If an update breaks the build, either fix the code or pin to the previous version
   - Use `go mod vendor` to ensure reproducible builds
   - Commit `go.mod` and `go.sum` changes together with any required code changes

6. **Handling Breaking Changes**:
   - Review changelogs and migration guides for major version updates
   - Create separate commits for dependency updates vs. code adaptations
   - Test with MongoDB and MQTT broker if updating related dependencies
   - Verify container builds still work after updates

**Go Language Version Updates:**

1. **Regular Version Checks**:
   - Check for Go language updates during ongoing development work
   - Monitor Go release notes for security patches and improvements
   - Current project requirement: Go 1.23+

2. **Update Strategy - Prioritize Stability**:
   - **Security Patches**: Apply Go patch releases promptly after testing
   - **Minor Version Updates**: Evaluate and test thoroughly before upgrading
   - **Major Version Updates**: Plan carefully, may require code changes
   - **Never break the build**: Always verify builds succeed before committing version changes
   - Test on all target platforms (amd64, armhf, arm64, ppc64le, riscv64, s390x)

3. **Testing After Go Version Updates**:
   ```bash
   # Update go.mod
   go mod edit -go=1.22  # Example version
   go mod tidy
   
   # Verify build
   go build ./...
   
   # Run tests
   go test ./...
   go test -race ./...
   
   # Run coverage
   ./runCoverage.sh
   
   # Test container builds
   ./buildContainer.sh amd64
   ```

4. **Version Update Guidelines**:
   - Update `go.mod` file with new Go version
   - Update documentation (README.md, AGENTS.md) with new version requirement
   - Update CI/CD configurations (.travis.yml, GitHub Actions)
   - Update Dockerfiles if they specify Go version
   - Run full test suite including race detection
   - Verify all container builds succeed
   - Document any code changes required for the new version

5. **Backward Compatibility**:
   - Maintain compatibility with previous minor version when possible
   - Document minimum required Go version clearly
   - Test builds with both old and new versions during transition

### Configuration Management

**Configuration Implementation Requirements:**

1. **Dual Configuration Support**: All configuration parameters MUST support both:
   - Configuration file entries (e.g., `sync.conf`)
   - Environment variables
   - Environment variables take precedence over file settings

2. **Configuration Layering**:
   - Default values defined in code (see `SetDefaultConfig()` in `common/config.go`)
   - Configuration file values override defaults
   - Environment variables override both defaults and file values
   - This allows flexible deployment across different environments

3. **Adding New Configuration Parameters**:
   - Add field to `Config` struct in `common/config.go` with both tags:
     ```go
     NewParameter string `env:"NEW_PARAMETER"`
     ```
   - Add default value in `SetDefaultConfig()` function
   - **CRITICAL**: Document in `sync.conf` with:
     - Description of the parameter
     - Default value
     - Environment variable name
     - Example usage
     - Appropriate section placement (Basic, Security, Storage, etc.)
   - **IMPORTANT**: Also update all sample configuration files in `samples/` directory to maintain consistency
     - `samples/send-receive-files/css-http.conf`
     - `samples/send-receive-files/css-local-broker.conf`
     - `samples/send-receive-files/ess-http.conf`
     - `samples/send-receive-files/ess-local-broker.conf`
   - Add validation in `ValidateConfig()` if needed

**Configuration File Synchronization:**
- **ALWAYS** update `sync.conf` when adding, modifying, or removing configuration parameters in `Config` struct
- Configuration files are the primary documentation for users - they must stay synchronized with code
- When changing defaults in `SetDefaultConfig()`, update the default values documented in all `.conf` files
- When changing validation logic in `ValidateConfig()`, update the parameter descriptions to reflect new constraints
- Configuration changes without corresponding `.conf` updates create confusion and support issues

**Security Configuration Parameters:**

The following configuration parameters control path validation security (CWE-22 protection):

- `AllowedCertificateExtensions`: Array of allowed file extensions for certificate files
  - Default: `[".pem", ".crt", ".cert"]`
  - Environment variable: `ALLOWED_CERTIFICATE_EXTENSIONS`
  - Used for validating: ServerCertificate, MQTTCACertificate, MQTTSSLCert, MongoCACertificate, HTTPCSSCACertificate

- `AllowedKeyExtensions`: Array of allowed file extensions for private key files
  - Default: `[".pem", ".key"]`
  - Environment variable: `ALLOWED_KEY_EXTENSIONS`
  - Used for validating: ServerKey, MQTTSSLKey

These parameters centralize extension validation across all file operations, making it easy to customize allowed file types for different deployment environments.

4. **Configuration Best Practices**:
   - Use clear, descriptive parameter names
   - Provide sensible defaults for optional parameters
   - Document all parameters thoroughly in `sync.conf`
   - Validate configuration values early (in `ValidateConfig()`)
   - Use appropriate types (bool, int, string, etc.)
   - Group related configuration parameters together

5. **Path Configuration**:
   - All paths relative to `PersistenceRootPath` unless absolute
   - Default paths: `/var/edge-sync-service/` or `/var/wiotp-edge/persist/`
   - Support both relative and absolute path specifications
   - Validate paths exist or can be created during startup

6. **Linux Filesystem Hierarchy Standard (FHS) Compliance**:
   - On Linux systems, user-specific configuration may come from the user's home directory structure
   - The Linux Filesystem Hierarchy Standard (FHS), maintained by the LSB (Linux Standard Base) workgroup within the Linux Foundation, defines standard directory structures
   - **User Configuration Locations** (in order of precedence):
     - `~/.config/edge-sync-service/` - User-specific configuration files (XDG Base Directory Specification)
     - `~/.edge-sync-service/` - Alternative user-specific configuration location
     - `/etc/edge-sync-service/` - System-wide configuration (default)
   - **FHS Standard Provisions**:
     - `/etc/` - System-wide configuration files
     - `/var/` - Variable data files (logs, databases, runtime state)
     - `/usr/local/` - Locally installed software and configuration
     - `~/.config/` - User-specific application configuration (XDG standard)
     - `~/.local/share/` - User-specific application data
   - **Implementation Considerations**:
     - Check user home directory configuration before falling back to system-wide defaults
     - Respect `XDG_CONFIG_HOME` environment variable if set (defaults to `~/.config`)
     - Respect `XDG_DATA_HOME` environment variable if set (defaults to `~/.local/share`)
     - Ensure proper file permissions when reading from user directories
     - Document configuration file search order in user-facing documentation
   - **Reference**: [Filesystem Hierarchy Standard](https://refspecs.linuxfoundation.org/fhs.shtml) maintained by the Linux Foundation

### Security Considerations

1. **TLS/SSL**: ESS always uses HTTPS; CSS can use HTTP or HTTPS
2. **Certificate Validation**: 
   - Never set `AllowInvalidCertificates` in production
   - Support for certificate pinning via SHA256 fingerprints
3. **Path Traversal Protection (CWE-22)**: 
   - **CRITICAL**: All file operations MUST use path validation functions
   - Use `common.ValidateFilePath()` for general file path validation
   - Use `common.ValidateFilePathWithExtension()` for certificate/key files
   - Path validation protects against:
     - Path traversal attacks (../, ../../, etc.)
     - Null byte injection (CWE-158)
     - Symlink attacks (CWE-61)
     - Access to files outside allowed directories
   - **Implementation locations**:
     - `common/pathvalidation.go`: Core validation functions
     - `common/config.go`: Startup validation in `ValidateConfig()`
     - `core/security/`: Authentication file path validation
     - `core/storage/`: MongoDB certificate path validation
     - `core/communications/`: MQTT certificate path validation
     - `core/base/`: Server certificate path validation
   - **Usage example**:
     ```go
     // Validate general file path
     validPath, err := common.ValidateFilePath(userPath, baseDir)
     if err != nil {
         return fmt.Errorf("invalid path: %w", err)
     }
     
     // Validate certificate file with extension check
     certExts := []string{".pem", ".crt", ".cert"}
     validCert, err := common.ValidateFilePathWithExtension(certPath, baseDir, certExts)
     if err != nil {
         return fmt.Errorf("invalid certificate path: %w", err)
     }
     ```
   - **Testing requirements**:
     - All path validation changes MUST include security tests
     - Test path traversal attempts (../, absolute paths outside base)
     - Test null byte injection
     - Test symlink attacks (when applicable)
     - See `common/pathvalidation_test.go` for examples
   - `AllowedDataFileExtensions` restricts accessible file types for data operations
4. **SSRF Protection**: 
   - `AllowPrivateIPs` setting controls access to private IP ranges
   - Default is `true` for backward compatibility

### Testing Practices

**Test-First Development Methodology**

This project follows a test-first development approach:

1. **Tests Define Expected Behavior**: Write tests that describe the correct, desired behavior of the system
   - Tests should reflect what the code SHOULD do, not what it currently does
   - Never modify test expectations to match existing functional deficiencies
   - If a test fails due to incorrect implementation, fix the implementation, not the test

2. **Functional Improvements Over Test Adjustments**: When tests reveal issues:
   - **Correct Approach**: Improve the implementation to make the test pass
   - **Incorrect Approach**: Change the test to match the broken behavior
   - Exception: Only adjust tests if the original test expectations were genuinely incorrect

3. **Test-Driven Bug Fixes**:
   - Write a failing test that demonstrates the bug
   - Verify the test fails with current code
   - Fix the implementation to make the test pass
   - Never adjust the test to accept the buggy behavior

4. **Example Scenario**:
   ```
   BAD:  Test expects validation to reject invalid input
         → Implementation doesn't validate
         → Change test to accept invalid input ❌
   
   GOOD: Test expects validation to reject invalid input
         → Implementation doesn't validate
         → Add validation to implementation ✓
   ```

5. **When to Adjust Tests**:
   - Original test expectations were based on misunderstanding requirements
   - Requirements have legitimately changed
   - Test was testing implementation details rather than behavior
   - Never adjust tests simply because implementation is difficult to fix

**CRITICAL: Test Coverage Requirements**

All source code changes MUST be accompanied by corresponding test updates:

1. **New Features**: Add comprehensive test cases covering:
   - Happy path scenarios
   - Edge cases and boundary conditions
   - Error handling paths
   - Concurrent access patterns (with race detection)

2. **Bug Fixes**: MANDATORY test requirements:
   - Add a test case that reproduces the bug BEFORE fixing it
   - Verify the test fails with the bug present
   - Verify the test passes after the fix
   - Document the bug scenario in test comments

3. **Security Fixes**: CRITICAL test requirements:
   - Add test cases demonstrating the vulnerability (safely)
   - Test both the attack vector and the mitigation
   - Include tests for bypass attempts
   - Document CVE or security issue reference if applicable
   - Examples: Path traversal tests, SSRF tests, certificate validation tests

4. **Refactoring**: Ensure existing tests still pass and add tests for:
   - New code paths introduced
   - Changed behavior or interfaces
   - Performance characteristics if relevant

**Test Isolation Principle:**

All tests MUST be designed with proper isolation to ensure reliability and maintainability:

1. **Independent Test Execution**: Each test must be able to run independently without relying on:
   - Execution order of other tests
   - State left behind by previous tests
   - Shared global state that persists between tests

2. **Clean Test Environment**: 
   - Use `t.TempDir()` for temporary file operations (automatically cleaned up)
   - Create isolated storage instances (in-memory or temporary BoltDB)
   - Reset or mock global state in `setUp` functions
   - Clean up resources in `defer` statements or `t.Cleanup()`

3. **Avoid Test Interdependencies**:
   - Never assume test execution order
   - Each test should set up its own required state
   - Don't share test data structures between test cases
   - Use table-driven tests with independent test cases

4. **Parallel Test Safety**:
   - Mark tests as parallel-safe with `t.Parallel()` when appropriate
   - Ensure parallel tests don't share mutable state
   - Use separate storage instances for concurrent tests
   - Test with race detector: `go test -race`

5. **External Service Isolation**:
   - Use mock implementations for external dependencies (MongoDB, MQTT)
   - Provide test-specific configuration to avoid conflicts
   - Use unique database names or collections for integration tests
   - Clean up test data after integration tests complete

6. **Test Data Isolation**:
   - Generate unique test data for each test case (e.g., unique IDs, timestamps)
   - Avoid hardcoded test data that could conflict across tests
   - Use test-specific prefixes or namespaces for identifiers
   - Clean up test data immediately after test completion

7. **Configuration Isolation**:
   - Create test-specific configuration instances
   - Never modify global configuration in tests
   - Use configuration mocks or test doubles when needed
   - Reset configuration state in test cleanup

8. **Time and Randomness Isolation**:
   - Mock time-dependent functions for deterministic tests
   - Use fixed seeds for random number generators in tests
   - Avoid tests that depend on wall-clock time
   - Make time-sensitive tests configurable with timeouts

9. **Network and I/O Isolation**:
   - Mock network calls and external API interactions
   - Use in-memory implementations instead of real I/O when possible
   - Avoid tests that depend on external network availability
   - Use local test servers or mock servers for integration tests

10. **Example of Good Test Isolation**:
    ```go
    func TestStorageOperation(t *testing.T) {
        t.Parallel() // Safe to run in parallel
        
        // Create isolated test environment
        tempDir := t.TempDir() // Auto-cleanup
        store := storage.NewInMemoryStorage() // Isolated instance
        
        // Generate unique test data
        testID := fmt.Sprintf("test-%d", time.Now().UnixNano())
        
        // Clean up any resources
        t.Cleanup(func() {
            store.Stop()
        })
        
        // Test logic with isolated state
        // ...
    }
    ```

**Test Cleanup and Environmental Responsibility:**

All tests MUST clean up after themselves and avoid leaving destructive changes to the environment:

1. **Complete Cleanup**: Every test must restore the environment to its original state
   - Delete temporary files and directories created during testing
   - Close all open file handles, network connections, and database connections
   - Remove test data from databases and storage systems
   - Restore modified configuration or global state
   - Use `defer` statements or `t.Cleanup()` to ensure cleanup happens even on test failure

2. **No Destructive Side Effects**: Tests must not:
   - Modify files outside the test's temporary directory
   - Delete or overwrite production data or configuration
   - Leave processes running after test completion
   - Consume system resources indefinitely (memory leaks, goroutine leaks)
   - Modify shared system state that affects other tests or processes

3. **Idempotent Tests**: Tests should be repeatable without manual cleanup
   - Running the same test multiple times should produce the same results
   - Tests should not depend on previous test runs being cleaned up
   - Use unique identifiers to avoid conflicts with concurrent test runs

4. **Resource Management**:
   - Always close resources in `defer` or `t.Cleanup()` blocks
   - Use context with timeout for operations that might hang
   - Monitor and clean up goroutines to prevent leaks
   - Release locks and semaphores properly

5. **Example of Proper Cleanup**:
   ```go
   func TestWithProperCleanup(t *testing.T) {
       // Create temporary directory (auto-cleanup)
       tempDir := t.TempDir()
       
       // Create test database connection
       db, err := openTestDB()
       require.NoError(t, err)
       defer db.Close() // Ensure connection is closed
       
       // Register cleanup for test data
       t.Cleanup(func() {
           // Remove test data from database
           db.DeleteTestData(testID)
           // Stop any background processes
           stopTestProcesses()
       })
       
       // Test logic here
       // ...
   }
   ```

**When Creating or Updating Tests:**

- **Always verify test isolation**: Run the test multiple times in different orders
- **Test in parallel**: Use `go test -parallel=10` to expose isolation issues
- **Check for race conditions**: Always run `go test -race` on concurrent code
- **Verify cleanup**: Ensure no test artifacts remain after test completion
- **Check for resource leaks**: Monitor goroutines, file handles, and memory usage
- **Document dependencies**: Clearly document any external service requirements
- **Use subtests for variations**: Group related test cases with `t.Run()` for better organization

**Test File Conventions:**
- Unit tests in `*_test.go` files alongside source
- Integration tests require external services (MongoDB, MQTT broker)
- Race detection enabled for concurrency testing: `go test -race`
- Mock implementations available for testing (e.g., `testAuthenticate.go`)
- Test storage uses in-memory or temporary BoltDB instances

**Test Naming:**
- Use descriptive test names: `TestFunctionName_Scenario_ExpectedBehavior`
- Security tests: Prefix with vulnerability type (e.g., `TestRetrieveData_PathTraversal_Blocked`)
- Bug fix tests: Reference issue number if available (e.g., `TestIssue123_NullPointerFix`)

**Test Documentation Requirements:**

All test functions MUST include comprehensive documentation above the function declaration:

1. **Documentation Structure**:
   - Start with a clear one-line summary of what the test validates
   - List specific test cases and scenarios covered
   - Explain security implications and CWE references where applicable
   - Include usage notes (e.g., "run with -race detector")
   - Explain why the test is critical for the system

2. **Documentation Format**:
   ```go
   // TestFunctionName_Scenario tests [brief description]:
   // - Specific test case 1
   // - Specific test case 2
   // - Edge case or boundary condition
   //
   // Additional context about security implications, CWE references,
   // or why this test is critical for production systems.
   //
   // Usage notes: Run with go test -race for concurrency tests.
   func TestFunctionName_Scenario(t *testing.T) {
       // Test implementation
   }
   ```

3. **Required Documentation Elements**:
   - **What**: Clear description of functionality being tested
   - **How**: List of specific test cases and scenarios
   - **Why**: Security implications, CWE references, or business criticality
   - **Usage**: Special requirements (race detector, external services, etc.)

4. **Security Test Documentation**:
   - MUST include CWE reference numbers (e.g., CWE-22, CWE-158, CWE-918)
   - MUST explain the attack vector being tested
   - MUST explain the mitigation being validated
   - MUST note if test demonstrates vulnerability safely

5. **Examples of Good Test Documentation**:
   ```go
   // TestValidateFilePath_PathTraversal tests path validation against traversal attacks:
   // - Parent directory traversal (../, ../../)
   // - Absolute paths outside base directory
   // - Null byte injection (CWE-158)
   // - Symlink attacks (CWE-61)
   //
   // This test ensures protection against CWE-22: Path Traversal attacks,
   // preventing unauthorized access to files outside allowed directories.
   // Critical for maintaining filesystem security boundaries.
   func TestValidateFilePath_PathTraversal(t *testing.T) { ... }
   
   // TestAuthentication_ConcurrentAccess tests concurrent authentication:
   // - 50 goroutines attempting authentication simultaneously
   // - Verifies no race conditions in authentication logic
   // - Ensures consistent results across concurrent calls
   //
   // Run with: go test -race
   // Critical for production environments with high concurrent user load.
   func TestAuthentication_ConcurrentAccess(t *testing.T) { ... }
   ```

6. **Documentation Benefits**:
   - Makes test purpose immediately clear to reviewers
   - Helps maintainers understand security implications
   - Provides context for why tests exist
   - Documents attack vectors and mitigations
   - Serves as inline security documentation

7. **When to Update Documentation**:
   - When adding new test cases to existing tests
   - When fixing bugs that tests should have caught
   - When security vulnerabilities are discovered
   - When test behavior or scope changes

### Common Pitfalls

1. **MongoDB Dependency**: Many tests fail without MongoDB running
2. **Path Configuration**: Relative paths resolved against `PersistenceRootPath`
3. **Race Conditions**: Always run race detector when modifying concurrent code
4. **Certificate Handling**: Self-signed certs auto-generated for ESS
5. **Communication Protocol**: ESS supports only one protocol; CSS supports hybrid modes
6. **Missing Tests**: Never commit code changes without corresponding test updates

### API Documentation

Generate Swagger documentation:
```bash
swagger generate spec -o ./swagger.json -m -b ./cmd/edge-sync-service
```

View documentation:
```bash
$GOPATH/bin/edge-sync-service -swagger swagger.json
# Open browser to http://<host>:<port>/swagger
```

### Logging and Tracing

- Configurable log levels: NONE, STATUS, FATAL, ERROR, WARNING, INFO, DEBUG, TRACE
- Multiple destinations: file, stdout, syslog, glog
- Separate log and trace files with automatic rotation
- Default location: `/var/edge-sync-service/log` and `/var/edge-sync-service/trace`

### Performance Tuning

Key configuration parameters for high-load scenarios:
- `MQTTParallelMode`: Enable parallel MQTT message processing (small/medium/large)
- `MaxInflightChunks`: Control concurrent chunk transfers (10-100 recommended)
- `MongoSessionCacheSize`: Increase for high update rates (32-512)
- `ObjectQueueBufferSize`: Adjust queue sizes based on workload

## Important Notes for AI Agents
## Efficient Tool Usage and Batching Strategies

### Principles of Efficient Tool Usage

When working with code changes, especially large-scale modifications, agents should optimize tool usage to minimize context usage, reduce costs, and improve performance:

1. **Batch Similar Operations**: Group similar changes together in a single tool call when possible
2. **Use the Right Tool**: Choose tools that can handle multiple operations efficiently
3. **Minimize Round Trips**: Reduce the number of tool calls by combining operations
4. **Plan Before Executing**: Analyze the scope of work before starting to identify batching opportunities

### Batching Strategies for Large Changes

**When to Use Batching:**

Batching is most effective for:
- Adding documentation to multiple functions in the same file
- Applying the same pattern across multiple files
- Making consistent formatting or style changes
- Adding similar test cases across multiple test files
- Updating configuration in multiple locations

**Tools That Support Batching:**

1. **apply_diff**: Can apply multiple search/replace blocks in a single call
   - Each block operates independently within the same file
   - Ideal for making multiple small, precise changes to one file
   - Example: Adding documentation to 5-10 functions in the same file
   - Each search/replace block should be separated by a blank line

2. **replace_regex**: Can apply multiple regex patterns in a single call
   - Supports multiple pattern/replacement pairs in one diff
   - Ideal for consistent pattern-based changes
   - Example: Updating import statements or renaming patterns across a file

**Batching Best Practices:**

1. **Group by File**: Batch all changes for a single file into one tool call
   - Good: One apply_diff call with 10 search/replace blocks for 10 functions
   - Bad: 10 separate apply_diff calls for the same file

2. **Limit Batch Size**: Keep batches manageable (5-15 operations per call)
   - Too small: Wastes tool calls and increases context usage
   - Too large: Harder to debug if one operation fails
   - Sweet spot: 8-12 related operations per batch

3. **Verify Before Batching**: Read the file first to ensure all targets exist
   - Use read_file to examine the file structure
   - Confirm all search strings will match exactly
   - Plan the batch based on actual file content

4. **Handle Failures Gracefully**: If a batch fails, break it into smaller batches
   - Identify which operation failed
   - Complete successful operations first
   - Retry failed operations individually or in smaller groups

**Example: Efficient Documentation Addition**

Instead of 10 separate tool calls:
```
# Inefficient: 10 separate apply_diff calls
<apply_diff> func1 </apply_diff>
<apply_diff> func2 </apply_diff>
...
<apply_diff> func10 </apply_diff>
```

Use one batched call:
```
# Efficient: 1 apply_diff call with 10 blocks
<apply_diff>
<file_path>/path/to/file.go</file_path>
<diff>
# Search: |||
func Function1() {
|||
# Replace with: |||
// Function1 does something important
func Function1() {
|||

# Search: |||
func Function2() {
|||
# Replace with: |||
// Function2 does something else
func Function2() {
|||

... (8 more blocks)
</diff>
</apply_diff>
```

**When NOT to Batch:**

Avoid batching when:
- Operations depend on each other (one must complete before the next)
- Changes span multiple files (use separate tool calls per file)
- Operations are unrelated or serve different purposes
- Debugging is needed (smaller operations are easier to troubleshoot)
- The batch would exceed 15-20 operations (too complex)

**Real-World Example from This Project:**

When documenting test functions across the edge-sync-service project:
- **Efficient approach**: Used apply_diff with 10-15 function documentation blocks per file
- **Result**: Documented 25 test files with ~60 tool calls instead of ~200+
- **Savings**: ~70% reduction in tool calls, significant context and cost savings
- **Maintainability**: Each file's changes were atomic and easy to review

### Performance Optimization Tips

1. **Read Once, Write Once**: Read a file once, plan all changes, apply in one batch
2. **Use Appropriate Tools**: 
   - `apply_diff` for precise, literal replacements
   - `replace_regex` for pattern-based changes
   - `write_to_file` only for complete file rewrites
3. **Minimize File Reads**: Cache file content mentally during planning phase
4. **Parallel Planning**: While waiting for tool responses, plan the next batch
5. **Progressive Refinement**: Start with a small batch to verify approach, then scale up



1. **Always check MongoDB availability** before running tests that require it
2. **Use appropriate storage provider** based on node type (CSS vs ESS)
3. **Respect the security model** - never bypass authentication in production code
4. **Follow the queue-based architecture** for object processing
5. **Consider multi-tenancy** when working on CSS features
6. **Test with race detector** when modifying concurrent code
7. **Validate configuration** using `ValidateConfig()` before starting services
8. **Handle graceful shutdown** via `Stop()` with appropriate quiesce time
9. **NEVER commit code without tests** - especially for bug fixes and security patches
10. **Security fixes require explicit test coverage** demonstrating both the vulnerability and the fix

## AI Agent Limitations

**DO NOT attempt to provide or calculate:**

1. **Timelines and Schedules**: Do not estimate project timelines, development schedules, or completion dates
2. **Performance Metrics**: Do not calculate or estimate:
   - Latency targets or measurements
   - Throughput rates or capacity
   - Hit rates or cache efficiency
   - Response times or processing speeds
   - Resource utilization percentages
3. **Risk Evaluations**: Do not assess or quantify:
   - Security risk levels or scores
   - Business impact assessments
   - Probability of failures or incidents
   - Cost-benefit analyses
4. **Quantitative Predictions**: Avoid making numerical predictions about system behavior, user adoption, or operational metrics

**Why these limitations exist:**
- AI agents cannot accurately perform these calculations without real-world data
- Estimates and predictions require domain expertise, historical data, and context that agents lack
- Providing inaccurate numbers creates false confidence and can lead to poor decisions
- These assessments require human judgment, stakeholder input, and organizational context

**What agents CAN do:**
- Identify areas where performance testing is needed
- Suggest monitoring and measurement approaches
- Recommend best practices for performance optimization
- Point to relevant documentation or tools for proper assessment
- Implement code changes based on clear, specific requirements
