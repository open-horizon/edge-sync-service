package dataURI

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

// Error is the error used in the data URI package
type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// validateDataPath ensures the resolved file path is within the allowed base directory
func validateDataPath(resolvedPath string, baseDir string) error {
	// Ensure baseDir is absolute and clean
	absBaseDir, err := filepath.Abs(filepath.Clean(baseDir))
	if err != nil {
		return fmt.Errorf("failed to resolve base directory: %v", err)
	}
	
	// Resolve symlinks in base directory
	realBaseDir, err := filepath.EvalSymlinks(absBaseDir)
	if err != nil {
		// If base dir symlink resolution fails, use original
		realBaseDir = absBaseDir
	}
	
	// Check if the resolved path is within the base directory
	relPath, err := filepath.Rel(realBaseDir, resolvedPath)
	if err != nil {
		return fmt.Errorf("failed to compute relative path: %v", err)
	}
	
	// If the relative path starts with "..", it's outside the base directory
	if strings.HasPrefix(relPath, "..") || strings.HasPrefix(relPath, string(filepath.Separator)) {
		return fmt.Errorf("path traversal detected: %s is outside allowed directory %s", resolvedPath, realBaseDir)
	}
	
	// Additional check: ensure no null bytes in path (can bypass some checks)
	if strings.Contains(resolvedPath, "\x00") {
		return fmt.Errorf("null byte detected in path")
	}
	
	return nil
}


// AppendData appends a chunk of data to the file stored at the given URI
func AppendData(uri string, dataReader io.Reader, dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool, isTempData bool) (bool, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data chunk at %s", uri)
	}

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return isLastChunk, &Error{"Invalid data URI"}
	}

	baseFilePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return isLastChunk, &Error{fmt.Sprintf("Failed to resolve file path %v", baseFilePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(baseFilePath, common.Configuration.PersistenceRootPath); err != nil {
		return isLastChunk, &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	tmpFilePath := baseFilePath + ".tmp"

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Open file %s", tmpFilePath)
	}
	file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return isLastChunk, common.CreateError(err, fmt.Sprintf("Failed to open file %s to append data. Error: ", tmpFilePath))
	}
	defer closeFileLogError(file)
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return isLastChunk, &common.IOError{Message: fmt.Sprintf("Failed to seek to the offset %d of a file. Error: %s", offset, err.Error())}
	}

	written, err := io.Copy(file, dataReader)
	if err != nil && err != io.EOF {
		return isLastChunk, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) {
		return isLastChunk, &common.IOError{Message: "Failed to write all the data to file."}
	}

	fileInfo, err := os.Stat(tmpFilePath)
	if err != nil {
		return isLastChunk, &common.IOError{Message: "Failed to check file size. Error: " + err.Error()}
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("File size after append %d is %d", offset, fileInfo.Size())
	}

	if isLastChunk && !isTempData {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Rename file from %s to %s", tmpFilePath, baseFilePath)
		}
		if err := os.Rename(tmpFilePath, baseFilePath); err != nil {
			return isLastChunk, &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
		}
	}
	return isLastChunk, nil
}

// StoreData writes the data to the file stored at the given URI
func StoreData(uri string, dataReader io.Reader, dataLength uint32) (int64, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data at %s", uri)
	}

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return 0, &Error{"Invalid data URI"}
	}

	baseFilePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return 0, &Error{fmt.Sprintf("Failed to resolve file path %v", baseFilePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(baseFilePath, common.Configuration.PersistenceRootPath); err != nil {
		return 0, &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	tmpFilePath := baseFilePath + ".tmp"

	file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to write data. Error: ", tmpFilePath))
	}
	defer closeFileLogError(file)

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return 0, &common.IOError{Message: "Failed to seek to the start of a file. Error: " + err.Error()}
	}

	written, err := io.Copy(file, dataReader)
	if err != nil && err != io.EOF {
		return 0, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) && dataLength != 0 {
		return 0, &common.IOError{Message: "Failed to write all the data to file."}
	}
	if err := os.Rename(tmpFilePath, baseFilePath); err != nil {
		return 0, &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
	}
	return written, nil
}

// StoreTempData writes the data to the tmp file stored at the given URI
func StoreTempData(uri string, dataReader io.Reader, dataLength uint32) (int64, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data at %s", uri)
	}

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return 0, &Error{"Invalid data URI"}
	}

	baseFilePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return 0, &Error{fmt.Sprintf("Failed to resolve file path %v", baseFilePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(baseFilePath, common.Configuration.PersistenceRootPath); err != nil {
		return 0, &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	tmpFilePath := baseFilePath + ".tmp"

	file, err := os.OpenFile(tmpFilePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to write data. Error: ", tmpFilePath))
	}
	defer closeFileLogError(file)

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return 0, &common.IOError{Message: "Failed to seek to the start of a file. Error: " + err.Error()}
	}

	written, err := io.Copy(file, dataReader)
	if err != nil && err != io.EOF {
		return 0, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) && dataLength != 0 {
		return 0, &common.IOError{Message: "Failed to write all the data to file."}
	}
	return written, nil
}

// StoreDataFromTempData rename {dataURI.Path}.tmp to {dataURI.Path}
func StoreDataFromTempData(uri string) common.SyncServiceError {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data from temp data at %s", uri)
	}

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return &Error{"Invalid data URI"}
	}

	baseFilePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return &Error{fmt.Sprintf("Failed to resolve file path %v", baseFilePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(baseFilePath, common.Configuration.PersistenceRootPath); err != nil {
		return &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	tmpFilePath := baseFilePath + ".tmp"

	if err := os.Rename(tmpFilePath, baseFilePath); err != nil {
		return &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
	}

	return nil
}

// GetData retrieves the data stored at the given URI.
// After reading, the reader has to be closed.
func GetData(uri string, isTempData bool) (io.Reader, common.SyncServiceError) {
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return nil, &Error{"Invalid data URI"}
	}

	filePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return nil, &Error{fmt.Sprintf("Failed to resolve file path %v", filePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(filePath, common.Configuration.PersistenceRootPath); err != nil {
		return nil, &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	if isTempData {
		filePath = filePath + ".tmp"
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving data from %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &common.NotFound{}
		}
		return nil, common.CreateError(err, fmt.Sprintf("Failed to open file %s to read data. Error: ", filePath))
	}
	return file, nil
}

// GetDataChunk retrieves the data stored at the given URI.
// After reading, the reader has to be closed.
func GetDataChunk(uri string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return nil, false, 0, &Error{"Invalid data URI"}
	}

	filePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return nil, false, 0, &Error{fmt.Sprintf("Failed to resolve file path %v", filePath)}
	}

	// Resolve symlinks to prevent symlink attacks
	realPath, err := filepath.EvalSymlinks(filePath)
	if err != nil {
		if trace.IsLogging(logger.WARNING) {
			trace.Warning("Could not resolve symlinks for %s: %v", filePath, err)
		}
		realPath = filePath
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(realPath, common.Configuration.PersistenceRootPath); err != nil {
		return nil, false, 0, &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	// Optional: Validate file extension if whitelist is configured (CWE-73 protection)
	if len(common.Configuration.AllowedDataFileExtensions) > 0 {
		ext := strings.ToLower(filepath.Ext(realPath))
		allowed := false
		for _, allowedExt := range common.Configuration.AllowedDataFileExtensions {
			if ext == strings.ToLower(allowedExt) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, false, 0, &Error{fmt.Sprintf("File extension %s not allowed", ext)}
		}
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving data from %s", realPath)
	}

	file, err := os.Open(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, 0, &common.NotFound{}
		}
		return nil, true, 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to read data. Error: ", realPath))
	}
	defer closeFileLogError(file)

	// Validate file type - must be regular file
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, true, 0, &common.IOError{Message: "Failed to stat file: " + err.Error()}
	}

	if !fileInfo.Mode().IsRegular() {
		return nil, true, 0, &Error{fmt.Sprintf("Invalid file type: %s is not a regular file", realPath)}
	}

	eof := false
	result := make([]byte, size)
	n, err := file.ReadAt(result, offset)
	if n == size {
		if err != nil { // This, most probably, can never happen when n == size, but the doc doesn't say it
			return nil, true, 0, &common.IOError{Message: "Failed to read data. Error: " + err.Error()}
		}
		var fi os.FileInfo
		fi, err = file.Stat()
		if err == nil && fi.Size() == offset+int64(size) {
			eof = true
		}
	} else {
		// err != nil is always true when n<size
		if err == io.EOF {
			eof = true
		} else {
			return nil, true, 0, &common.IOError{Message: "Failed to read data. Error: " + err.Error()}
		}
	}

	return result, eof, n, nil
}

// DeleteStoredData deletes the data file stored at the given URI
func DeleteStoredData(uri string, isTempData bool) common.SyncServiceError {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting stored data at %s, isTempData: %t", uri, isTempData)
	}

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return &Error{"Invalid data URI"}
	}

	filePath, err := filepath.Abs(filepath.Clean(dataURI.Path))
	if err != nil {
		return &Error{fmt.Sprintf("Failed to resolve file path %v", filePath)}
	}

	// Validate path to prevent directory traversal
	if err := validateDataPath(filePath, common.Configuration.PersistenceRootPath); err != nil {
		return &Error{fmt.Sprintf("Invalid file path: %v", err)}
	}

	if isTempData {
		filePath = filePath + ".tmp"
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting %s", filePath)
	}

	if err = os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return &common.IOError{Message: "Failed to delete data. Error: " + err.Error()}
	}
	return nil
}

// closeFileLogError will close the given file and log any error encountered if logging is enabled
func closeFileLogError(f *os.File) {
	if f == nil {
		return
	}
	if err := f.Close(); err != nil {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Error closing file: %v", err.Error())
		}
	}
}
