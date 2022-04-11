package dataVerifier

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
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

	var dr io.Reader
	var publicKeyBytes []byte
	var signatureBytes []byte
	var err error

	if dataVerifier.writeThrough {
		dr = data
	} else {
		if publicKeyBytes, err = base64.StdEncoding.DecodeString(dataVerifier.publicKey); err != nil {
			return false, &common.InternalError{Message: "PublicKey is not base64 encoded. Error: " + err.Error()}
		} else if signatureBytes, err = base64.StdEncoding.DecodeString(dataVerifier.signature); err != nil {
			return false, &common.InternalError{Message: "Signature is not base64 encoded. Error: " + err.Error()}
		} else {

			dr = io.TeeReader(data, dataVerifier.dataHash)
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		if dataVerifier.writeThrough {
			trace.Debug("DataVerifier - Pass-thru mode for object %s %s\n", objectType, objectID)
		} else {
			trace.Debug("DataVerifier - In VerifyDataSignature, verifying and storing data for object %s %s\n", objectType, objectID)
		}
	}

	if destinationDataURI != "" {
		if _, err := dataURI.StoreData(destinationDataURI, dr, 0); err != nil {
			return false, err
		}
	} else {
		if exists, err := Store.StoreObjectData(orgID, objectType, objectID, dr); err != nil || !exists {
			return false, err
		}
	}

	if dataVerifier.writeThrough {
		return true, nil
	} else {
		return dataVerifier.verifyHelper(publicKeyBytes, signatureBytes)
	}
}

// GetTempData is to get temp data for data verification
func (dataVerifier *DataVerifier) GetTempData(metaData common.MetaData) (io.Reader, common.SyncServiceError) {
	var dr io.Reader
	var err common.SyncServiceError
	if metaData.DestinationDataURI != "" {
		dr, err = dataURI.GetData(metaData.DestinationDataURI, true)
	} else {
		dr, err = Store.RetrieveObjectTempData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	}

	if err != nil {
		return nil, err
	}

	return dr, nil
}

// CleanUp function is to clean up the temp file created during data verification
func (dataVerifier *DataVerifier) RemoveTempData(orgID string, objectType string, objectID string, destinationDataURI string) common.SyncServiceError {
	if destinationDataURI != "" {
		if err := dataURI.DeleteStoredData(destinationDataURI, true); err != nil {
			return err
		}
	} else if err := Store.RemoveObjectTempData(orgID, objectType, objectID); err != nil {
		return err
	}
	return nil
}

func (dataVerifier *DataVerifier) RemoveUnverifiedData(metaData common.MetaData) common.SyncServiceError {
	return storage.DeleteStoredData(Store, metaData)
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
