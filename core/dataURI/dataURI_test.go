package dataURI

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

// TestDataURI tests data URI operations for file-based storage:
// - AppendData stores data at specified URI with offset
// - GetData retrieves data from URI
// - StoreData replaces entire data at URI
// - DeleteStoredData removes data from URI
// - Multi-chunk data appending (first, middle, last chunks)
// - GetDataChunk retrieves data with offset and size limits
//
// This comprehensive test ensures that the data URI abstraction works correctly
// for file-based storage operations. Data URIs provide a uniform interface for
// storing and retrieving object data regardless of the underlying storage mechanism.
// Critical for handling large file transfers in chunks, which is essential for
// efficient bandwidth usage in edge computing scenarios.
//
// The test uses temporary directories to ensure isolation and automatic cleanup.
func TestDataURI(t *testing.T) {
	// Create temporary directory for test data
	tmpDir, err := ioutil.TempDir("", "datauri-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set PersistenceRootPath to temp directory
	originalPath := common.Configuration.PersistenceRootPath
	common.Configuration.PersistenceRootPath = tmpDir
	defer func() {
		common.Configuration.PersistenceRootPath = originalPath
	}()

	dir := tmpDir
	tests := []struct {
		uri        string
		data       []byte
		dataLength uint32
		offset     int64
	}{
		{"file:///" + dir + "/test1.txt", []byte("hello"), 5, 0},
	}

	for _, row := range tests {
		if _, err := AppendData(row.uri, bytes.NewReader(row.data), row.dataLength, row.offset, 0, true, true, false); err != nil {
			t.Errorf("Failed to store in data uri. Error: %s", err.Error())
		} else {
			if dataReader, err := GetData(row.uri, false); err != nil {
				t.Errorf("Failed to read from data uri. Error: %s", err.Error())
			} else {
				storedData := make([]byte, 100)
				n, err := dataReader.Read(storedData)
				if err != nil {
					t.Errorf("Failed read from data uri. Error: %s", err.Error())
				} else {
					if n != len(row.data) {
						t.Errorf("Read incorrect number of bytes from data uri: %d instead of %d", n, len(row.data))
					} else {
						storedData = storedData[:n]
						if string(storedData) != string(row.data) {
							t.Errorf("Read incorrect data: %s instead of %s", string(storedData), string(row.data))
						}
					}
				}
				switch v := dataReader.(type) {
				case *os.File:
					if err := v.Close(); err != nil {
						t.Errorf("Failed to close reader. Error: %s", err.Error())
					}
				}
			}
		}
	}

	for _, row := range tests {
		if written, err := StoreData(row.uri, bytes.NewReader(row.data), row.dataLength); err != nil {
			t.Errorf("Failed to store in data uri. Error: %s", err.Error())
		} else {
			if dataReader, err := GetData(row.uri, false); err != nil {
				t.Errorf("Failed to read from data uri. Error: %s", err.Error())
			} else {
				if written != int64(row.dataLength) {
					t.Errorf("Incorrect length of written data: %d instead of %d", written, row.dataLength)
				}
				storedData := make([]byte, 100)
				n, err := dataReader.Read(storedData)
				if err != nil {
					t.Errorf("Failed read from data uri. Error: %s", err.Error())
				} else {
					if n != len(row.data) {
						t.Errorf("Read incorrect number of bytes from data uri: %d instead of %d", n, len(row.data))
					} else {
						storedData = storedData[:n]
						if string(storedData) != string(row.data) {
							t.Errorf("Read incorrect data: %s instead of %s", string(storedData), string(row.data))
						}
					}
				}
				switch v := dataReader.(type) {
				case *os.File:
					if err := v.Close(); err != nil {
						t.Errorf("Failed to close reader. Error: %s", err.Error())
					}
				}
			}
		}
		if err = DeleteStoredData(row.uri, false); err != nil {
			t.Errorf("Failed to delete stored data. Error: %s", err.Error())
		} else {
			if dataReader, err := GetData(row.uri, false); err == nil && dataReader != nil {
				t.Errorf("Read from deleted data uri")
			}
		}
	}

	chunk1 := []byte("Hello")
	chunk2 := []byte(" world")
	chunk3 := []byte("!")

	testsMulti := []struct {
		uri        string
		chunks     [][]byte
		wholeData  []byte
		lengths    []uint32
		dataLength uint32
		offsets    []int64
	}{
		{"file:///" + dir + "/test2.txt", [][]byte{chunk1}, []byte("Hello"), []uint32{5}, 5, []int64{0}},
		{"file:///" + dir + "/test3.txt", [][]byte{chunk1, chunk2, chunk3}, []byte("Hello world!"), []uint32{5, 6, 1}, 12, []int64{0, 5, 11}},
	}

	for _, row := range testsMulti {
		isFirstChunk := true
		isLastChunk := false
		if len(row.chunks) == 1 {
			isLastChunk = true
		}
		for i, chunk := range row.chunks {
			if _, err := AppendData(row.uri, bytes.NewReader(chunk), row.lengths[i], row.offsets[i], int64(len(row.wholeData)), isFirstChunk, isLastChunk, false); err != nil {
				t.Errorf("Failed to store in data uri. Error: %s", err.Error())
			}
			isFirstChunk = false
			if i >= len(row.chunks)-2 {
				isLastChunk = true
			}
		}
		if dataReader, err := GetData(row.uri, false); err != nil {
			t.Errorf("Failed to read from data uri. Error: %s", err.Error())
		} else {
			storedData := make([]byte, 100)
			n, err := dataReader.Read(storedData)
			if err != nil {
				t.Errorf("Failed read from data uri. Error: %s", err.Error())
			} else {
				if n != int(row.dataLength) {
					t.Errorf("Read incorrect number of bytes from data uri: %d instead of %d", n, row.dataLength)
				} else {
					storedData = storedData[:n]
					if string(storedData) != string(row.wholeData) {
						t.Errorf("Read incorrect data: %s instead of %s", string(storedData), string(row.wholeData))
					}
				}
			}
			switch v := dataReader.(type) {
			case *os.File:
				if err := v.Close(); err != nil {
					t.Errorf("Failed to close reader. Error: %s", err.Error())
				}
			}
		}

		// Read with offset
		for i := 0; ; i += 3 {
			chunk, eof, n, err := GetDataChunk(row.uri, 3, int64(i))

			if err != nil {
				t.Errorf("Failed read chunk from data uri. Error: %s", err.Error())
			}
			if eof {
				if i+3 < len(row.wholeData) {
					t.Errorf("GetDataChunk returned EOF")
					return
				}
			}
			if n < 3 && !eof {
				t.Errorf("GetDataChunk returned chunk smaller that the required size")
			}
			chunk = chunk[:n]
			if string(chunk) != string(row.wholeData[i:i+n]) {
				t.Errorf("Read incorrect data: %s instead of %s", string(chunk), string(row.wholeData[i:i+n]))
			}
			if eof {
				break
			}
		}

		if err = DeleteStoredData(row.uri, false); err != nil {
			t.Errorf("Failed to delete %s. Error: %s", row.uri, err)
		}
	}
}
