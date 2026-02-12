package dataVerifier

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

const destinationURI = "/dataURITmp"

var destinationURIDir string
var destinationURIDirFileVerified string
var destinationURIDirFileWrong string

var dataToSign, wrongDataToSign []byte
var orgID, objectType, objectID string

// TestNewDataVerifier tests DataVerifier initialization with different hash algorithms:
// - SHA1 hash algorithm
// - SHA256 hash algorithm
// - Validates writeThrough flag behavior with invalid/missing parameters
//
// This ensures that the DataVerifier is correctly initialized with supported
// hash algorithms and properly handles edge cases like missing public keys or
// signatures. The writeThrough flag determines whether data is written directly
// to storage or buffered for verification first.
func TestNewDataVerifier(t *testing.T) {
	testNewDataVerifier(common.Sha1, t)
	testNewDataVerifier(common.Sha256, t)
}

// TestVerifyDataSignature tests data signature verification:
// - Verifying data with correct signature (should pass)
// - Verifying data with incorrect signature (should fail)
// - Storing verified data to storage
// - Testing with both SHA1 and SHA256 hash algorithms
// - Testing with both MongoDB and BoltDB storage backends
//
// This ensures that RSA-PSS signature verification works correctly, which is
// critical for ensuring data integrity and authenticity in edge computing scenarios.
// Only data with valid signatures from trusted sources should be accepted and stored.
//
// Run with: go test -v (includes MongoDB tests)
// Run with: go test -v -short (skips MongoDB tests)
func TestVerifyDataSignature(t *testing.T) {
	setupTestVars()

	if !testing.Short() {
		if status := setupDB(common.Mongo); status != "" {
			t.Errorf("Failed to setup %s storage, error: %s", common.Mongo, status)
		}
		defer Store.Stop()
		testVerifyDataSignature(common.Sha1, t)
		testVerifyDataSignature(common.Sha256, t)
	} else {
		t.Log("Skipping MongoDB test in short mode")
	}

	if status := setupDB(common.Bolt); status != "" {
		t.Errorf("Failed to setup %s storage, error: %s", common.Bolt, status)
	}
	defer Store.Stop()
	testVerifyDataSignature(common.Sha1, t)
	testVerifyDataSignature(common.Sha256, t)
}

// testNewDataVerifier tests DataVerifier initialization with specific hash algorithm:
// - Tests writeThrough flag with invalid hash algorithm
// - Tests writeThrough flag with missing public key
// - Tests writeThrough flag with missing signature
// - Tests writeThrough flag with valid parameters
//
// This helper function validates that the DataVerifier correctly sets the
// writeThrough flag based on parameter validity. When writeThrough is true,
// data is written directly to storage without verification (used when
// verification parameters are invalid or missing).
func testNewDataVerifier(hashAlgo string, t *testing.T) {
	dataToSign := []byte("dataVerifier test")

	var publicKey, signature string
	var err error
	if publicKey, signature, err = setupDataSignature(dataToSign, hashAlgo); err != nil {
		t.Errorf("Failed to set up publicKey and signature with %s for data. Error: %s\n", hashAlgo, err.Error())
	}

	var dataVerifierToTest *DataVerifier
	dataVerifierToTest = NewDataVerifier("", publicKey, signature)
	if !dataVerifierToTest.writeThrough {
		t.Error("\"writeThrough\" field should be true if hash algorithm is not SHA1 and SHA256")
	}

	dataVerifierToTest = NewDataVerifier(hashAlgo, "", signature)
	if !dataVerifierToTest.writeThrough {
		t.Error("\"writeThrough\" field should be true if publicKey is empty")
	}

	dataVerifierToTest = NewDataVerifier(hashAlgo, publicKey, "")
	if !dataVerifierToTest.writeThrough {
		t.Error("\"writeThrough\" field should be true if signature is empty")
	}

	dataVerifierToTest = NewDataVerifier(hashAlgo, publicKey, signature)
	if dataVerifierToTest.writeThrough {
		t.Error("\"writeThrough\" field should be false with valid input")
	}
}

// testVerifyDataSignature tests signature verification with specific hash algorithm:
// - Stores object metadata with signature information
// - Verifies data with incorrect signature (should fail)
// - Verifies data with correct signature (should pass)
// - Confirms verified data is stored in storage
//
// This helper function performs the actual signature verification testing
// for a specific hash algorithm. It ensures that only data with valid
// signatures is accepted and stored, implementing CWE-345: Insufficient
// Verification of Data Authenticity protection.
func testVerifyDataSignature(hashAlgo string, t *testing.T) {
	var publicKey, signature string
	var err error
	if publicKey, signature, err = setupDataSignature(dataToSign, hashAlgo); err != nil {
		t.Errorf("Failed to set up publicKey and signature with %s for data. Error: %s\n", hashAlgo, err.Error())
	}

	dataVerifier := NewDataVerifier(hashAlgo, publicKey, signature)

	// Store object metadata
	objMetaData := common.MetaData{
		ObjectID:      objectID,
		ObjectType:    objectType,
		DestOrgID:     orgID,
		HashAlgorithm: hashAlgo,
		PublicKey:     publicKey,
		Signature:     signature,
		//DataVerified:  false,
	}

	// Store object metadata
	if _, err := Store.StoreObject(objMetaData, []byte{}, ""); err != nil {
		t.Errorf("Failed to store object metadata, error: %s", err.Error())
	}

	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(wrongDataToSign), orgID, objectType, objectID, ""); err == nil || verified {
		errMessage := ""
		if err != nil {
			errMessage = err.Error()
		}
		t.Errorf("Error verifying data, wrong data should not pass verification. verified: %t, error: %s\n", verified, errMessage)
	}

	// Need another dataVerifier object because re-use old object will make the hash calculated on top of the hash from old object
	dataVerifier = NewDataVerifier(hashAlgo, publicKey, signature)
	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(dataToSign), orgID, objectType, objectID, ""); err != nil || !verified {
		t.Errorf("Error verifying data, data should pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	var reader io.Reader
	if reader, err = Store.RetrieveObjectData(orgID, objectType, objectID, false); err != nil {
		t.Errorf("Error retrieve verified data for %s %s %s, error: %s\n", orgID, objectType, objectID, err.Error())
	} else if reader == nil {
		Store.CloseDataReader(reader)
		t.Errorf("Object data for %s %s %s should be stored after verification\n", orgID, objectType, objectID)
	}
	Store.CloseDataReader(reader)

}

// TestVerifyDataSignatureWithDestintionDataURI tests signature verification with destination URIs:
// - Verifying data and writing to destination URI on success
// - Verifying data and NOT writing to destination URI on failure
// - Removing unverified data from destination URI
// - Testing with both SHA1 and SHA256 hash algorithms
// - Testing with both MongoDB and BoltDB storage backends
//
// This ensures that data signature verification works correctly when writing
// to destination URIs (file paths). Verified data is written to the destination,
// while unverified data is removed. Critical for ensuring only authenticated
// data reaches its destination in edge computing scenarios.
//
// Run with: go test -v (includes MongoDB tests)
// Run with: go test -v -short (skips MongoDB tests)
func TestVerifyDataSignatureWithDestintionDataURI(t *testing.T) {
	setupDataURIPath()
	setupTestVars()
	destinationURIDirFileVerified = "file:///" + destinationURIDir + "/" + "test_verified.txt"
	destinationURIDirFileWrong = "file:///" + destinationURIDir + "/" + "test_wrong.txt"

	if !testing.Short() {
		if status := setupDB(common.Mongo); status != "" {
			t.Errorf("Failed to setup %s storage, error: %s", common.Mongo, status)
		}
		defer Store.Stop()
		testVerifyDataSignatureWithDestintionDataURI(common.Sha1, t)
		testVerifyDataSignatureWithDestintionDataURI(common.Sha256, t)
	} else {
		t.Log("Skipping MongoDB test in short mode")
	}

	if status := setupDB(common.Bolt); status != "" {
		t.Errorf("Failed to setup %s storage, error: %s", common.Bolt, status)
	}
	defer Store.Stop()
	testVerifyDataSignatureWithDestintionDataURI(common.Sha1, t)
	testVerifyDataSignatureWithDestintionDataURI(common.Sha256, t)

}

// testVerifyDataSignatureWithDestintionDataURI tests signature verification with destination URIs for specific hash algorithm:
// - Sets up two objects with destination URIs (one for valid data, one for invalid)
// - Verifies valid data and confirms file is created at destination URI
// - Verifies invalid data and confirms file is created (but should be removed)
// - Removes unverified data using RemoveUnverifiedData
//
// This helper function validates that the DataVerifier correctly handles
// destination URIs during verification, writing verified data to the specified
// location and providing mechanisms to clean up unverified data.
func testVerifyDataSignatureWithDestintionDataURI(hashAlgo string, t *testing.T) {
	var publicKey, signature string
	var err error
	if publicKey, signature, err = setupDataSignature(dataToSign, hashAlgo); err != nil {
		t.Errorf("Failed to set up publicKey and signature with SHA1 for data. Error: %s\n", err.Error())
	}

	objectID1 := "testDVObjID1"
	objectID2 := "testDVObjID2"

	metaData1, err := setupObjectForVerify(objectID1, publicKey, signature, hashAlgo, destinationURIDirFileVerified)
	if err != nil {
		t.Errorf("Failed to set up object(objectID=%s) for testing. Error: %s\n", objectID1, err.Error())
	}

	metaData2, err := setupObjectForVerify(objectID2, publicKey, signature, hashAlgo, destinationURIDirFileWrong)
	if err != nil {
		t.Errorf("Failed to set up object(objectID=%s) for testing. Error: %s\n", objectID2, err.Error())
	}

	// Verify Signature
	dataVerifier := NewDataVerifier(hashAlgo, publicKey, signature)
	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(dataToSign), metaData1.DestOrgID, metaData1.ObjectType, metaData1.ObjectID, metaData1.DestinationDataURI); err != nil || !verified {
		t.Errorf("Error verifying data, data should pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(wrongDataToSign), metaData2.DestOrgID, metaData2.ObjectType, metaData2.ObjectID, metaData2.DestinationDataURI); err == nil || verified {
		t.Errorf("Error verifying data, wrong data should not pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	// check .tmp file is created
	if _, err := os.Stat(destinationURIDir + "/test_verified.txt"); err != nil {
		t.Errorf("Error checking files at destinationURI %s, error: %s\n", metaData1.DestinationDataURI, err.Error())
	}

	if _, err := os.Stat(destinationURIDir + "/test_wrong.txt"); err != nil {
		t.Errorf("Error checking files at destinationURI %s, error: %s\n", metaData2.DestinationDataURI, err.Error())
	}

	if err = dataVerifier.RemoveUnverifiedData(*metaData2); err != nil {
		t.Errorf("Error remove tmp data for %s %s %s at %s, error: %s\n", metaData2.DestOrgID, metaData2.ObjectType, metaData2.ObjectID, metaData2.DestinationDataURI, err.Error())
	}

}

// setupTestVars initializes test variables:
// - dataToSign: Valid test data for signature verification
// - wrongDataToSign: Invalid test data that should fail verification
// - orgID, objectType, objectID: Test object identifiers
//
// This helper function provides consistent test data across all test cases,
// ensuring reproducible test results.
func setupTestVars() {
	dataToSign = []byte("dataVerifier test")
	wrongDataToSign = []byte("wrong data")
	orgID = "testDVOrg"
	objectType = "testDVObjType"
	objectID = "testDVObjID"
}

// setupObjectForVerify creates and stores an object for verification testing:
// - Creates object metadata with signature information
// - Stores object in storage backend
// - Returns metadata for use in tests
//
// This helper function sets up test objects with all necessary signature
// verification parameters, enabling consistent test setup across different
// test cases.
func setupObjectForVerify(objectID string, publicKey string, signature string, hashAlgo string, destinationURI string) (*common.MetaData, common.SyncServiceError) {
	objMetaDataToStore := common.MetaData{
		ObjectID:           objectID,
		ObjectType:         objectType,
		DestOrgID:          orgID,
		HashAlgorithm:      hashAlgo,
		PublicKey:          publicKey,
		Signature:          signature,
		DestinationDataURI: destinationURI,
	}

	// Store object metadata
	if _, err := Store.StoreObject(objMetaDataToStore, []byte{}, ""); err != nil {
		return nil, err
	}
	return &objMetaDataToStore, nil

}

// setupDB initializes a storage backend for testing:
// - Mongo: Initializes MongoDB storage with test database
// - Bolt: Initializes BoltDB storage with cleanup
// - InMemory: Initializes in-memory storage (fallback)
//
// This helper function provides a consistent way to initialize different
// storage backends for testing, ensuring tests can run against all supported
// storage types. Returns empty string on success or error message on failure.
func setupDB(dbType string) string {
	if dbType == common.Mongo {
		common.Configuration.MongoDbName = "d_test_db"
		Store = &storage.MongoStorage{}
	} else if dbType == common.Bolt {
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup(true)
		Store = boltStore
	} else {
		fmt.Println("set inmemory storage")
		Store = &storage.InMemoryStorage{}
	}

	if err := Store.Init(); err != nil {
		return fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	return ""
}

// setupDataURIPath creates temporary directory structure for data URI testing:
// - Creates persist directory for path validation compliance
// - Creates dataURITmp subdirectory for test files
// - Sets up file paths for verified and unverified test data
// - Configures PersistenceRootPath for path validation
//
// This helper function ensures proper directory structure for data URI tests,
// complying with path validation requirements (CWE-22 protection) by creating
// directories within the configured PersistenceRootPath.
//
// Returns empty string on success or error message on failure.
func setupDataURIPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Sprintf("Failed to get current directory. Error: %s\n", err.Error())
	}

	// Create persist directory structure to satisfy path validation
	persistDir := dir + "/persist"
	err = os.MkdirAll(persistDir, 0750)
	if err != nil {
		return fmt.Sprintf("Failed to create persist directory. Error: %s\n", err.Error())
	}

	// Set PersistenceRootPath to persist directory
	common.Configuration.PersistenceRootPath = persistDir
	
	// Create dataURITmp within persist directory
	destinationURIDir = persistDir + destinationURI
	err = os.MkdirAll(destinationURIDir, 0750)
	if err != nil {
		return fmt.Sprintf("Failed to initialize dataURI temp folder. Error: %s\n", err.Error())
	}

	destinationURIDirFileVerified = "file:///" + destinationURIDir + "/" + "test_verified.txt"
	destinationURIDirFileWrong = "file:///" + destinationURIDir + "/" + "test_wrong.txt"

	return ""
}

// setupDataSignature generates RSA key pair and signs data:
// - Generates 2048-bit RSA private key
// - Extracts and encodes public key as base64 string
// - Computes hash of data using specified algorithm (SHA1 or SHA256)
// - Signs hash using RSA-PSS signature scheme
// - Returns base64-encoded public key and signature
//
// This helper function creates valid cryptographic signatures for testing
// data verification. Uses RSA-PSS (Probabilistic Signature Scheme) which
// provides better security properties than traditional RSA signatures.
//
// Returns: (publicKey, signature, error)
func setupDataSignature(data []byte, hashAlgo string) (string, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	publicKey := &privateKey.PublicKey
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", "", err
	}
	publicKeyString := base64.StdEncoding.EncodeToString(publicKeyBytes)

	var dataHash hash.Hash
	var cyrptoHash crypto.Hash
	if hashAlgo == common.Sha1 {
		dataHash = sha1.New()
		cyrptoHash = crypto.SHA1
	} else {
		dataHash = sha256.New()
		cyrptoHash = crypto.SHA256
	}

	_, err = dataHash.Write(data)
	if err != nil {
		return "", "", err
	}
	dataHashSum := dataHash.Sum(nil)

	signature, err := rsa.SignPSS(rand.Reader, privateKey, cyrptoHash, dataHashSum, nil)
	if err != nil {
		return "", "", err
	}
	signatureString := base64.StdEncoding.EncodeToString(signature)
	return publicKeyString, signatureString, nil
}
