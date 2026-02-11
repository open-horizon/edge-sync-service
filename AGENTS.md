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
