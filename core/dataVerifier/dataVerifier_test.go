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

func TestNewDataVerifier(t *testing.T) {
	testNewDataVerifier(common.Sha1, t)
	testNewDataVerifier(common.Sha256, t)
}

func TestVerifyDataSignature(t *testing.T) {
	setupTestVars()

	if status := setupDB(common.Mongo); status != "" {
		t.Errorf("Failed to setup %s storage, error: %s", common.Mongo, status)
	}
	defer Store.Stop()
	testVerifyDataSignature(common.Sha1, t)
	testVerifyDataSignature(common.Sha256, t)

	if status := setupDB(common.Bolt); status != "" {
		t.Errorf("Failed to setup %s storage, error: %s", common.Bolt, status)
	}
	defer Store.Stop()
	testVerifyDataSignature(common.Sha1, t)
	testVerifyDataSignature(common.Sha256, t)
}

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

func testVerifyDataSignature(hashAlgo string, t *testing.T) {
	var publicKey, signature string
	var err error
	if publicKey, signature, err = setupDataSignature(dataToSign, hashAlgo); err != nil {
		t.Errorf("Failed to set up publicKey and signature with %s for data. Error: %s\n", hashAlgo, err.Error())
	}

	dataVerifier := NewDataVerifier(hashAlgo, publicKey, signature)
	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(wrongDataToSign), orgID, objectType, objectID, ""); err == nil || verified {
		t.Errorf("Error verifying data, wrong data should not pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	// Need another dataVerifier object because re-use old object will make the hash calculated on top of the hash from old object
	dataVerifier = NewDataVerifier(hashAlgo, publicKey, signature)
	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(dataToSign), orgID, objectType, objectID, ""); err != nil || !verified {
		t.Errorf("Error verifying data, data should pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	var reader io.Reader
	if reader, err = Store.RetrieveTempObjectData(orgID, objectType, objectID); err != nil {
		Store.CloseDataReader(reader)
		t.Errorf("Error get temp object data for %s %s %s, error: %s\n", orgID, objectType, objectID, err.Error())
	}
	Store.CloseDataReader(reader)

	// Store object metadata
	objMetaData := common.MetaData{
		ObjectID:      objectID,
		ObjectType:    objectType,
		DestOrgID:     orgID,
		HashAlgorithm: hashAlgo,
		PublicKey:     publicKey,
		Signature:     signature,
	}

	// Store object metadata
	if _, err := Store.StoreObject(objMetaData, []byte{}, ""); err != nil {
		t.Errorf("Failed to store object metadata, error: %s", err.Error())
	}

	// Store verified data
	if err = dataVerifier.StoreVerifiedData(orgID, objectType, objectID, ""); err != nil {
		t.Errorf("Error storeing verified data for %s %s %s, error: %s\n", orgID, objectType, objectID, err.Error())
	}

	if reader, err = Store.RetrieveTempObjectData(orgID, objectType, objectID); err != nil {
		t.Errorf("Error retrieve verified data for %s %s %s, error: %s\n", orgID, objectType, objectID, err.Error())
	} else if reader != nil {
		Store.CloseDataReader(reader)
		t.Errorf("Temp object data for %s %s %s should be deleted\n", orgID, objectType, objectID)
	}

	if reader, err = Store.RetrieveObjectData(orgID, objectType, objectID); err != nil {
		Store.CloseDataReader(reader)
		t.Errorf("Error get object data for %s %s %s, error: %s\n", orgID, objectType, objectID, err.Error())
	}
	Store.CloseDataReader(reader)

}

func TestVerifyDataSignatureWithDestintionDataURI(t *testing.T) {
	setupDataURIPath()
	setupTestVars()
	destinationURIDirFileVerified = "file:///" + destinationURIDir + "/" + "test_verified.txt"
	destinationURIDirFileWrong = "file:///" + destinationURIDir + "/" + "test_wrong.txt"

	testVerifyDataSignatureWithDestintionDataURI(common.Sha1, t)
	testVerifyDataSignatureWithDestintionDataURI(common.Sha256, t)

}

func testVerifyDataSignatureWithDestintionDataURI(hashAlgo string, t *testing.T) {
	var publicKey, signature string
	var err error
	if publicKey, signature, err = setupDataSignature(dataToSign, hashAlgo); err != nil {
		t.Errorf("Failed to set up publicKey and signature with SHA1 for data. Error: %s\n", err.Error())
	}

	// Verify Signature
	dataVerifier := NewDataVerifier(hashAlgo, publicKey, signature)
	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(dataToSign), orgID, objectType, objectID, destinationURIDirFileVerified); err != nil || !verified {
		t.Errorf("Error verifying data, data should pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	if verified, err := dataVerifier.VerifyDataSignature(bytes.NewReader(wrongDataToSign), orgID, objectType, objectID, destinationURIDirFileWrong); err == nil || verified {
		t.Errorf("Error verifying data, wrong data should not pass verification. verified: %t, error: %s\n", verified, err.Error())
	}

	// check .tmp file is created
	if _, err := os.Stat(destinationURIDir + "/test_verified.txt.tmp"); err != nil {
		t.Errorf("Error checking files at destinationURI %s, error: %s\n", destinationURIDirFileVerified, err.Error())
	}

	if _, err := os.Stat(destinationURIDir + "/test_wrong.txt.tmp"); err != nil {
		t.Errorf("Error checking files at destinationURI %s.tmp, error: %s\n", destinationURIDirFileWrong, err.Error())
	}

	// check file is created from .tmp file
	if err := dataVerifier.StoreVerifiedData(orgID, objectType, objectID, destinationURIDirFileVerified); err != nil {
		t.Errorf("Error storing verified data %s %s %s at destinationURI %s, error: %s\n", orgID, objectType, objectID, destinationURIDirFileVerified, err.Error())
	}
	if _, err := os.Stat(destinationURIDir + "/test_verified.txt.tmp"); !os.IsNotExist(err) {
		t.Errorf("The .tmp file at destinationURI %s should be removed, error: %s\n", destinationURIDir, err.Error())
	}
	if _, err := os.Stat(destinationURIDir + "/test_verified.txt"); err != nil {
		t.Errorf("Error checking files at destinationURI %s, error: %s\n", destinationURIDirFileVerified, err.Error())
	}
	if err = dataVerifier.RemoveTempData(orgID, objectType, objectID, destinationURIDirFileWrong); err != nil {
		t.Errorf("Error remove tmp data for %s %s %s at %s, error: %s\n", orgID, objectType, objectID, destinationURIDirFileWrong, err.Error())
	}

}

func setupTestVars() {
	dataToSign = []byte("dataVerifier test")
	wrongDataToSign = []byte("wrong data")
	orgID = "testDVOrg"
	objectType = "testDVObjType"
	objectID = "testDVObjID"
}

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

func setupDataURIPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Sprintf("Failed to get current directory. Error: %s\n", err.Error())
	}

	destinationURIDir = dir + destinationURI
	err = os.MkdirAll(destinationURIDir, 0750)
	if err != nil {
		return fmt.Sprintf("Failed to initialize dataURI temp folder. Error: %s\n", err.Error())
	}

	destinationURIDirFileVerified = "file:///" + destinationURIDir + "/" + "test_verified.txt"
	destinationURIDirFileWrong = "file:///" + destinationURIDir + "/" + "test_wrong.txt"

	return ""
}

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
