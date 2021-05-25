package dataVerifier

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash"
	"io"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	"github.com/open-horizon/edge-sync-service/core/storage"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type DataVerifier struct {
	dataHash       hash.Hash
	cryptoHashType crypto.Hash
	publicKey      string
	signature      string
	writeThrough   bool
}

// Store is a reference to the Storage being used
var Store storage.Storage

func NewDataVerifier(hashAlgorithm string, publicKey string, signature string) *DataVerifier {
	// default is to verify data (writeThrough == false)
	writeThrough := false
	var dataHash hash.Hash
	var cryptoHashType crypto.Hash
	var err error

	if !common.IsValidHashAlgorithm(hashAlgorithm) || publicKey == "" || signature == "" {
		writeThrough = true
	}

	if dataHash, cryptoHashType, err = common.GetHash(hashAlgorithm); err != nil {
		writeThrough = true
	}

	return &DataVerifier{
		dataHash:       dataHash,
		cryptoHashType: cryptoHashType,
		publicKey:      publicKey,
		signature:      signature,
		writeThrough:   writeThrough,
	}
}

// VerifyDataSignature is to verify the data. This function will generate the tmp data in storage. Call RemoveTempData() after verification to remove the tmp data
func (dataVerifier *DataVerifier) VerifyDataSignature(data io.Reader, orgID string, objectType string, objectID string, destinationDataURI string) (bool, common.SyncServiceError) {
	if dataVerifier.writeThrough {
		return true, nil
	}

	if publicKeyBytes, err := base64.StdEncoding.DecodeString(dataVerifier.publicKey); err != nil {
		return false, &common.InternalError{Message: "PublicKey is not base64 encoded. Error: " + err.Error()}
	} else if signatureBytes, err := base64.StdEncoding.DecodeString(dataVerifier.signature); err != nil {
		return false, &common.InternalError{Message: "Signature is not base64 encoded. Error: " + err.Error()}
	} else {
		dr := io.TeeReader(data, dataVerifier.dataHash)
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("DataVerifier - In VerifyDataSignature, verifying and storing temp data for object %s %s\n", objectType, objectID)
		}

		if destinationDataURI != "" {
			if _, err := dataURI.StoreTempData(destinationDataURI, dr, 0); err != nil {
				return false, err
			}
		} else {
			if exists, err := Store.StoreObjectTempData(orgID, objectType, objectID, dr); err != nil || !exists {
				return false, err
			}
		}

		return dataVerifier.verifyHelper(publicKeyBytes, signatureBytes)
	}
}

// StoreVerifiedData will store the data from temp data that generated during data verification. And remove temp data
func (dataVerifier *DataVerifier) StoreVerifiedData(orgID string, objectType string, objectID string, destinationDataURI string) common.SyncServiceError {
	if dataVerifier.writeThrough {
		return nil
	}

	if destinationDataURI != "" {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("DataVerifier - In StoreVerifiedData, store data from tmp data for object %s %s at URI %s\n", objectType, objectID, destinationDataURI)
		}
		// rename the {file}.tmp to {file}
		if err := dataURI.StoreDataFromTempData(destinationDataURI); err != nil {
			return err
		}
	} else {
		// 1. Retrieve temp data, 2. Store object data, 3. Remove temp data
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("DataVerifier - In StoreVerifiedData, retrieve temp data for object %s %s\n", objectType, objectID)
		}

		dataReader, err := Store.RetrieveTempObjectData(orgID, objectType, objectID)
		if err != nil {
			return &common.InvalidRequest{Message: "Failed to read temp data fro, Error: " + err.Error()}
		} else if dataReader == nil {
			return &common.InvalidRequest{Message: "Read empty temp data, Error: " + err.Error()}
		}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("DataVerifier - In StoreVerifiedData, storing data for object %s %s\n", objectType, objectID)
		}

		if exists, err := Store.StoreObjectData(orgID, objectType, objectID, dataReader); err != nil {
			Store.CloseDataReader(dataReader)
			return err
		} else if !exists {
			Store.CloseDataReader(dataReader)
			message := fmt.Sprintf("Object metadata is not found for object %s %s %s, Error: %s\n", orgID, objectType, objectID, err.Error())
			return &common.InternalError{Message: message}
		}
		Store.CloseDataReader(dataReader)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("DataVerifier - In StoreVerifiedData, remove temp data for object %s %s\n", objectType, objectID)
		}

		if err := Store.RemoveObjectTempData(orgID, objectType, objectID); err != nil {
			return err
		}
	}

	return nil

}

// CleanUp function is to clean up the temp file created during data verification
func (dataVerifier *DataVerifier) RemoveTempData(orgID string, objectType string, objectID string, destinationDataURI string) common.SyncServiceError {
	if destinationDataURI != "" {
		tmpFilePath := destinationDataURI + ".tmp"
		if err := dataURI.DeleteStoredData(tmpFilePath); err != nil {
			return err
		}
	} else if err := Store.RemoveObjectTempData(orgID, objectType, objectID); err != nil {
		return err
	}
	return nil
}

func (dataVerifier *DataVerifier) verifyHelper(publicKeyBytes []byte, signatureBytes []byte) (bool, common.SyncServiceError) {
	dataHashSum := dataVerifier.dataHash.Sum(nil)
	if pubKey, err := x509.ParsePKIXPublicKey(publicKeyBytes); err != nil {
		return false, &common.InternalError{Message: "Failed to parse public key, Error: " + err.Error()}
	} else {
		pubKeyToUse := pubKey.(*rsa.PublicKey)
		if err = rsa.VerifyPSS(pubKeyToUse, dataVerifier.cryptoHashType, dataHashSum, signatureBytes, nil); err != nil {
			return false, &common.InternalError{Message: "Failed to verify data with public key and data signature, Error: " + err.Error()}
		}
	}
	return true, nil
}
