package base

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	"github.com/open-horizon/edge-sync-service/core/dataVerifier"
	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-sync-service/core/storage"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

var apiLock sync.RWMutex
var apiObjectLocks common.Locks

func init() {
	apiObjectLocks = *common.NewLocks("api")
}

// UpdateObject invoked when an app sends an updated object
func UpdateObject(orgID string, objectType string, objectID string, metaData common.MetaData, data []byte) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In UpdateObject. Update %s %s %s\n", orgID, objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	if !common.IsValidName(orgID) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Organization ID (%s) contains invalid characters", orgID)}
	}

	// Verify that the object is valid
	if metaData.ObjectID == "" {
		return &common.InvalidRequest{Message: "Object's meta data does not contain object's ID"}
	}
	if objectID != metaData.ObjectID {
		message := fmt.Sprintf("Object ID (%+v) in the URL doesn't match the object-id (%+v) in the payload", objectID, metaData.ObjectID)
		return &common.InvalidRequest{Message: message}
	}
	if !common.IsValidName(objectID) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Object ID (%s) contains invalid characters", objectID)}
	}

	if metaData.ObjectType == "" {
		return &common.InvalidRequest{Message: "Object's meta data does not contain object's type"}
	}
	if objectType != metaData.ObjectType {
		message := fmt.Sprintf("Object type (%+v) in the URL doesn't match the object-type (%+v) in the payload", objectType, metaData.ObjectType)
		return &common.InvalidRequest{Message: message}
	}
	if !common.IsValidName(objectType) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Object type (%s) contains invalid characters", objectType)}
	}

	if metaData.HashAlgorithm != "" && metaData.HashAlgorithm != common.Sha1 && metaData.HashAlgorithm != common.Sha256 {
		return &common.InvalidRequest{Message: fmt.Sprintf("Hash Algorithm (%s) is not supported. Please use %s or %s if specify", metaData.HashAlgorithm, common.Sha1, common.Sha256)}
	}

	// verify publicKey and signature is base64 encoded
	if metaData.PublicKey != "" {
		if _, err := base64.StdEncoding.DecodeString(metaData.PublicKey); err != nil {
			return &common.InvalidRequest{Message: "PublicKey is not base64 encoded. Error: " + err.Error()}
		}
	}

	if metaData.Signature != "" {
		if _, err := base64.StdEncoding.DecodeString(metaData.Signature); err != nil {
			return &common.InvalidRequest{Message: "Signature is not base64 encoded. Error: " + err.Error()}
		}
	}

	if metaData.Expiration != "" {
		if common.Configuration.NodeType == common.ESS {
			return &common.InvalidRequest{Message: "Object expiration is disabled on ESS"}
		}

		expiration, err := time.Parse(time.RFC3339, metaData.Expiration)
		if err != nil {
			return &common.InvalidRequest{Message: "Failed to parse expiration in object's meta data. Error: " + err.Error()}
		}
		if time.Now().After(expiration) {
			return &common.InvalidRequest{Message: "Invalid expiration time in object's meta data"}
		}
	}

	if metaData.Version != "" && !common.IsValidName(metaData.Version) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Version (%s) contains one/some invalid characters (eg: <, >, =, ', \", &, space, \\, /)", metaData.Version)}
	}
	if metaData.Description != "" && common.IsInvalidDescription(metaData.Description) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Description (%s) contains one/some invalid characters (<, >, =, ', \", &, space, \\, /)", metaData.Description)}
	}

	if metaData.MetaOnly && len(data) != 0 {
		return &common.InvalidRequest{Message: "Can't update data if MetaOnly is true"}
	}

	if metaData.DestID != "" && metaData.DestType == "" {
		return &common.InvalidRequest{Message: "Destination ID provided without destination type in object's meta data"}
	}
	if metaData.DestinationsList != nil && common.Configuration.NodeType == common.ESS {
		return &common.InvalidRequest{Message: "Destinations list is not supported for ESS"}
	}
	if metaData.DestinationsList != nil && metaData.DestType != "" {
		return &common.InvalidRequest{Message: "Both destinations list and destination type are specified"}
	}
	if validatedDestList, _ := common.ValidateDestinationListInput(metaData.DestinationsList); validatedDestList == false {
		return &common.InvalidRequest{Message: "Unsupported char <, > in destinationsList."}
	}

	if metaData.DestinationPolicy != nil {
		if metaData.DestType != "" {
			return &common.InvalidRequest{Message: "Both destination policy and destination type are specified"}
		}

		if metaData.DestinationsList != nil {
			return &common.InvalidRequest{Message: "Both destination policy and destination list are specified"}
		}

		properties := metaData.DestinationPolicy.Properties
		for _, property := range properties {
			if len(property.Name) == 0 {
				return &common.InvalidRequest{Message: "A property in the DestinationPolicy must have a name"}
			}
		}

		services := metaData.DestinationPolicy.Services
		for _, service := range services {
			if len(service.OrgID) == 0 || len(service.Arch) == 0 || len(service.ServiceName) == 0 || len(service.Version) == 0 {
				return &common.InvalidRequest{
					Message: "A service in a DestinationPolicy must have an organization ID, architecture, service name, and version specified"}
			}

			if _, err := common.ParseSemVerRange(service.Version); err != nil {
				return &common.InvalidRequest{
					Message: fmt.Sprintf("A service in the DestinationPolicy has an invalid version `%s`", service.Version)}
			}
		}
	}

	if metaData.DestType != "" && !common.IsValidName(metaData.DestType) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Destination type (%s) contains invalid characters", metaData.DestType)}
	}

	if metaData.AutoDelete && metaData.DestinationsList == nil && metaData.DestID == "" {
		return &common.InvalidRequest{Message: "AutoDelete can be used only for objects with DestinationsList or DestID set"}
	}

	if metaData.ActivationTime != "" && metaData.Inactive {
		activation, err := time.Parse(time.RFC3339, metaData.ActivationTime)
		if err != nil {
			return &common.InvalidRequest{Message: "Failed to parse activation time in object's meta data. Error: " + err.Error()}
		}
		if time.Now().After(activation) {
			return &common.InvalidRequest{Message: "Invalid activation time in object's meta data"}
		}
	}

	if metaData.Deleted {
		return &common.InvalidRequest{Message: "Object marked as deleted"}
	}

	if metaData.DestinationDataURI != "" {
		if common.Configuration.NodeType == common.ESS {
			return &common.InvalidRequest{Message: "Data URI is disabled on CSS"}
		}

		uri, err := url.Parse(metaData.DestinationDataURI)
		if err != nil || !strings.EqualFold(uri.Scheme, "file") || uri.Host != "" {
			return &common.InvalidRequest{Message: "Invalid destination data URI"}
		}
	}

	if metaData.SourceDataURI != "" {
		if common.Configuration.NodeType == common.CSS {
			return &common.InvalidRequest{Message: "Data URI is disabled on CSS"}
		}
		if data != nil {
			return &common.InvalidRequest{Message: "Both source data URI and data are set"}
		}

		uri, err := url.Parse(metaData.SourceDataURI)
		if err != nil || !strings.EqualFold(uri.Scheme, "file") || uri.Host != "" {
			return &common.InvalidRequest{Message: "Invalid source data URI"}
		}
		if fi, err := os.Stat(uri.Path); err == nil {
			metaData.ObjectSize = fi.Size()
		} else {
			log.Error(" Invalid source data URI: %s, failed to get file information for the file, err= %v\n", metaData.SourceDataURI, err)
			return &common.InvalidRequest{Message: "Invalid source data URI"}
		}
	}

	if metaData.OriginType == "" || metaData.OriginID == "" {
		// Set the origin so the other side can respond
		metaData.OriginType = common.Configuration.DestinationType
		metaData.OriginID = common.Configuration.DestinationID
	} else {
		// metaData.OriginType != "" && metaData.OriginID != "", check if given valud is valid
		if !common.IsValidName(metaData.OriginType) {
			return &common.InvalidRequest{Message: fmt.Sprintf("OriginType (%s) contains one/some invalid characters (eg: <, >, =, ', \", &, space, \\, /)", metaData.OriginType)}
		} else if !common.IsValidName(metaData.OriginID) {
			return &common.InvalidRequest{Message: fmt.Sprintf("OriginID (%s) contains one/some invalid characters (eg: <, >, =, ', \", &, space, \\, /)", metaData.OriginID)}
		}
	}

	if metaData.DestOrgID == "" {
		metaData.DestOrgID = orgID
	} else if !common.IsValidName(metaData.DestOrgID) {
		return &common.InvalidRequest{Message: fmt.Sprintf("DestOrgID (%s) contains one/some invalid characters (eg: <, >, =, ', \", &, space, \\, /)", metaData.DestOrgID)}
	}

	if metaData.ExpectedConsumers == 0 {
		metaData.ExpectedConsumers = 1
	} else if metaData.ExpectedConsumers == -1 {
		metaData.ExpectedConsumers = math.MaxInt32
	}

	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	apiObjectLocks.Lock(lockIndex)
	common.ObjectLocks.Lock(lockIndex)

	existingObject, existingObjStatus, _ := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if existingObjStatus != "" && existingObjStatus != common.ReadyToSend && existingObjStatus != common.NotReadyToSend && existingObjStatus != common.Verifying && existingObjStatus != common.VerificationFailed {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Can't update object of the receiving side"}
	}

	// Store the object in the storage module
	status := common.NotReadyToSend
	if metaData.Link != "" || metaData.NoData || metaData.SourceDataURI != "" {
		status = common.ReadyToSend
		if metaData.NoData {
			data = nil
			metaData.Link = ""
			metaData.SourceDataURI = ""
			metaData.PublicKey = ""
			metaData.Signature = ""
		}
	} else if metaData.MetaOnly {
		// data is nil
		data = nil
		reader, err := store.RetrieveObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, false)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return err
		}
		if reader != nil {
			status = common.ReadyToSend
			store.CloseDataReader(reader)
		}
		// for MetaOnly, we will re-use the checksum fields
		if existingObject != nil {
			metaData.HashAlgorithm = existingObject.HashAlgorithm
			metaData.PublicKey = existingObject.PublicKey
			metaData.Signature = existingObject.Signature
		}
		// If no data in the database, then status is notReady (data == nil, status == notReady)
	}

	metaData.ChunkSize = common.Configuration.MaxDataChunkSize
	if data != nil {
		metaData.ObjectSize = int64(len(data))
		if !common.NeedDataVerification(metaData) {
			status = common.ReadyToSend
		}
	}

	// Verify
	// data signature verification if metadata has both publicKey and signature
	// data is nil for metaOnly object. Meta-only object will not apply data verification
	var deletedDestinations []common.StoreDestinationStatus
	var err common.SyncServiceError
	if data != nil && common.NeedDataVerification(metaData) {
		// Store metadata, with correct verified status
		deletedDestinations, err = store.StoreObject(metaData, nil, status)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return err
		}

		// Set object status from "notReady" to "verifying"
		status = common.Verifying
		if err = store.UpdateObjectStatus(orgID, objectType, objectID, status); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to update object status to %s for object %s/%s/%s", status, orgID, objectType, objectID)
			}
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return err
		}
		// will no store data if object metadata not exist
		dataReader := bytes.NewReader(data)
		dataVf := dataVerifier.NewDataVerifier(metaData.HashAlgorithm, metaData.PublicKey, metaData.Signature)
		if success, err := dataVf.VerifyDataSignature(dataReader, orgID, objectType, objectID, ""); !success || err != nil {
			if log.IsLogging(logger.ERROR) && err != nil {
				log.Error("Failed to verify data for object %s %s, Error: %s\n", objectType, objectID, err.Error())
			}

			dataVf.RemoveUnverifiedData(metaData)
			status = common.VerificationFailed
			if updateStatusErr := store.UpdateObjectStatus(orgID, objectType, objectID, status); updateStatusErr != nil && log.IsLogging(logger.ERROR) {
				log.Error("Failed to update object status to %s for %s %s, Error: %s\n", status, objectType, objectID, updateStatusErr.Error())
			}

			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return err
		} else {
			// verified, update object status to "ready"
			status = common.ReadyToSend
			if err = store.UpdateObjectStatus(orgID, objectType, objectID, status); err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error("Failed to update object status to %s for object %s/%s/%s", status, orgID, objectType, objectID)
				}
				common.ObjectLocks.Unlock(lockIndex)
				apiObjectLocks.Unlock(lockIndex)
				return err
			}
		}
	} else {
		// Store metadata and data, with correct verified status
		deletedDestinations, err = store.StoreObject(metaData, data, status)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	store.DeleteNotificationRecords(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, "", "")

	if status == common.NotReadyToSend || status == common.VerificationFailed || status == common.Verifying || metaData.Inactive {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return nil
	}

	// StoreObject increments the instance id, we need to fetch the updated meta data
	updatedMetaData, err := store.RetrieveObject(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)

	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	common.ObjectLocks.Unlock(lockIndex)
	apiObjectLocks.Unlock(lockIndex)
	//var deleteNotificationsInfo []common.NotificationInfo
	if len(deletedDestinations) != 0 {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObject. Send object to objectQueue for delete%s %s\n", objectType, objectID)
		}

		objectInQueue := common.ObjectInQueue{NotificationAction: common.Delete, NotificationType: common.TypeDestination, Object: *updatedMetaData, Destinations: deletedDestinations}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObject. Continue to updateDestination %s %s\n", objectType, objectID)
		}
	}

	// Should be in antoher thread
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In UpdateObject. Send object to objectQueue for update%s %s\n", objectType, objectID)
	}

	objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeObject, Object: *updatedMetaData, Destinations: []common.StoreDestinationStatus{}}
	objectQueue.SendObjectToQueue(objectInQueue)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In UpdateObject. Return response for UpdateObject %s %s\n", objectType, objectID)
	}

	return nil
}

// GetObjectStatus sends the status of the object to the app
// Call the storage module to get the status of the object and return it in the response
func GetObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetObjectStatus. Get status of %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	return store.RetrieveObjectStatus(orgID, objectType, objectID)
}

// ListUpdatedObjects provides a list of edge updated objects
// Call the storage module to get the list of edge updated objects and send it to the app
func ListUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	updatedObjects, err := store.RetrieveUpdatedObjects(orgID, objectType, received)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListUpdatedObjects. Get %s %s. returned %d objects\n", orgID, objectType, len(updatedObjects))
	}

	return updatedObjects, err
}

// ListObjectsWithDestinationPolicy provides a list of objects that have a DestinationPolicy
func ListObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveObjectsWithDestinationPolicy(orgID, received)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListObjectsWithDestinationPolicy. Get %s. Returned %d objects\n", orgID, len(objects))
	}

	return objects, err
}

// ListObjectsWithDestinationPolicyByService provides a list of objects that have a DestinationPolicy and are
// associated with a service
func ListObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListObjectsWithDestinationPolicyByService. Get %s/%s. Returned %d objects\n",
			serviceOrgID, serviceName, len(objects))
	}

	return objects, err
}

// ListObjectsWithDestinationPolicyUpdatedSince provides a list of objects that have a DestinationPolicy that has been updated since the specified time
func ListObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID, since)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListObjectsWithDestinationPolicyByService. Get %s since %d. Returned %d objects\n",
			orgID, since, len(objects))
	}

	return objects, err
}

// ListObjectsWithFilters provides a list of objects that satisfy the given conditions
func ListObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string, deleted *bool) ([]common.MetaData, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveObjectsWithFilters(orgID, destinationPolicy, dpServiceOrgID, dpServiceName, dpPropertyName, since, objectType, objectID, destinationType, destinationID, noData, expirationTimeBefore, deleted)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListObjectsWithFilters. Get %s. Returned %d objects\n", orgID, len(objects))
	}

	return objects, err
}

// ListAllObjects provides a list of all objects with the specified type
func ListAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveAllObjects(orgID, objectType)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListAllObjects. Get %s:%s. Returned %d objects\n", orgID, objectType, len(objects))
	}

	return objects, err
}

// GetObject delivers an object to the app
// Call the storage module to get the object's meta data and send it to the app
func GetObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetObject. Get %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	return store.RetrieveObject(orgID, objectType, objectID)
}

// GetObjectData delivers object data to the app
// Call the storage module to get the object's data and send it to the app
func GetObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetObjectData. Get data %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		return nil, err
	}
	if metaData == nil || status == common.NotReadyToSend || status == common.Verifying || status == common.VerificationFailed || status == common.ReceiverVerifying || status == common.ReceiverVerificationFailed || status == common.PartiallyReceived {
		return nil, nil
	}
	if metaData.DestinationDataURI != "" && status == common.CompletelyReceived {
		return dataURI.GetData(metaData.DestinationDataURI, false)
	}
	if metaData.SourceDataURI != "" && status == common.ReadyToSend {
		return dataURI.GetData(metaData.SourceDataURI, false)
	}
	return store.RetrieveObjectData(orgID, objectType, objectID, false)
}

// GetRemovedDestinationPolicyServicesFromESS get the removedDestinationPolicyServices list
// Call the storage module to get the object's removedDestinationPolicyServices
func GetRemovedDestinationPolicyServicesFromESS(orgID string, objectType string, objectID string) ([]common.ServiceID, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetRemovedDestinationPolicyServicesFromESS. Get RemovedDestinationPolicyServices for object %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	_, removedDestinationPolicyServices, err := store.RetrieveObjectAndRemovedDestinationPolicyServices(orgID, objectType, objectID)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetRemovedDestinationPolicyServicesFromESS. Get %d RemovedDestinationPolicyServices for object %s %s\n", len(removedDestinationPolicyServices), objectType, objectID)
	}
	return removedDestinationPolicyServices, err
}

// PutObjectAllData stores an object's data
// Verify data signature (if publicKey and signature both have value)
// Call the storage module to store the object's data
// Return true if the object was found and updated
// Return false and no error if the object was not found
func PutObjectAllData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectAllData. Update data %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, nil
	}
	if status != common.ReadyToSend && status != common.NotReadyToSend && status != common.Verifying && status != common.VerificationFailed {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data of the receiving side"}
	}
	if metaData.NoData {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data, the NoData flag is set to true"}
	}

	var dataVf *dataVerifier.DataVerifier
	if common.NeedDataVerification(*metaData) {
		// Set object status from "notReady" to "verifying"
		if err := store.UpdateObjectStatus(orgID, objectType, objectID, common.Verifying); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to update object status to %s for object %s/%s/%s", common.Verifying, orgID, objectType, objectID)
			}
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, &common.InternalError{Message: "Failed to update object status to verifying"}
		}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectAllData. Start data verification %s %s\n", objectType, objectID)
		}

		//start data verification. After verified, the data is stored. object size is updated, dataID is incremented
		dataVf = dataVerifier.NewDataVerifier(metaData.HashAlgorithm, metaData.PublicKey, metaData.Signature)
		if success, err := dataVf.VerifyDataSignature(dataReader, orgID, objectType, objectID, ""); !success || err != nil {
			errMessage := ""
			if log.IsLogging(logger.ERROR) {
				if err != nil {
					errMessage = err.Error()
				}
				log.Error("Failed to verify data for object %s %s, remove unverified data. Error: %s\n", objectType, objectID, errMessage)
			}
			dataVf.RemoveUnverifiedData(*metaData)

			if updateErr := store.UpdateObjectStatus(orgID, objectType, objectID, common.VerificationFailed); updateErr != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error("Failed to update object status to %s for object %s/%s/%s, Error: %s", common.VerificationFailed, orgID, objectType, objectID, updateErr.Error())
				}
			}
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, &common.InternalError{Message: "Failed to verify and store data, Error: " + errMessage}
		}

		if err := store.UpdateObjectStatus(orgID, objectType, objectID, common.ReadyToSend); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to update object status to %s for object %s/%s/%s", common.ReadyToSend, orgID, objectType, objectID)
			}
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, &common.InternalError{Message: "Failed to updated object status to " + common.ReadyToSend}
		}
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectAllData. data verified for object %s %s\n", objectType, objectID)
		}
	} else {
		// After the data is stored. object size is updated, dataID is incremented, object status is set to "ready"
		if exists, err := store.StoreObjectData(orgID, objectType, objectID, dataReader); err != nil || !exists {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, err
		}
	}

	if metaData.SourceDataURI != "" {
		if err = store.UpdateObjectSourceDataURI(orgID, objectType, objectID, ""); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, err
		}
	}

	var updatedMetaData *common.MetaData
	// StoreObject increments the instance id if this is a data update, we need to fetch the updated meta data
	// Also, StoreObjectData updates the ObjectSize, so we need to fetch the updated meta data
	updatedMetaData, err = store.RetrieveObject(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, err
	}

	if updatedMetaData.Inactive {
		// Don't send inactive objects to the other side
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return true, nil
	}

	// Should be in antoher thread
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectAllData. Send object to objectQueue %s %s\n", objectType, objectID)
	}

	common.ObjectLocks.Unlock(lockIndex)
	apiObjectLocks.Unlock(lockIndex)
	objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeObject, Object: *updatedMetaData, Destinations: []common.StoreDestinationStatus{}}
	objectQueue.SendObjectToQueue(objectInQueue)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectAllData. Return response for PutObjectData %s %s\n", objectType, objectID)
	}
	return true, nil
}

func PutObjectChunkData(orgID string, objectType string, objectID string, dataReader io.Reader, startOffset int64, endOffset int64, totalSize int64, uploadOwnerID string) (bool, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectChunkData. Update data %s %s %s, startOffset: %d, endOffset: %d. Check if is leader: %t. UploadOwnerID: %s, current CSS ID: %s\n", orgID, objectType, objectID, startOffset, endOffset, leader.CheckIfLeader(), uploadOwnerID, leader.GetLeaderID())
	}

	if !leader.CheckIfLeader() {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData. This is not leader, ignore...")
		}
		return false, &common.IgnoredRequest{Message: "Request Ignored by non-leader"}
	}

	if uploadOwnerID != "" && uploadOwnerID != leader.GetLeaderID() {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to put chunk data for %s %s %s. It is leader, but uploadOwnerID (%s) != CSSID (%s)", orgID, objectType, objectID, uploadOwnerID, leader.GetLeaderID())
		}
		return false, &common.InternalError{Message: "leader changed during the chunk uploading"}
	}

	// 2 situations when reach here:
	// It is leader && uploadOwnerID == leader.GetLeaderID()
	// It is leader && uploadOwnerID == ""

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, nil
	}
	if status != common.ReadyToSend && status != common.NotReadyToSend && status != common.Verifying && status != common.VerificationFailed {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data of the receiving side"}
	}
	if metaData.NoData {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data, the NoData flag is set to true"}
	}

	isFirstChunk := startOffset == 0
	isLastChunk := false
	dataSize := endOffset - startOffset + 1

	// append Data to temp file/data
	isTempData := false
	if common.NeedDataVerification(*metaData) {
		isTempData = true
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectChunkData for %s %s %s, isTempData: %t, isFirstChunk: %t, dataSize to store: %d \n", orgID, objectType, objectID, isTempData, isFirstChunk, dataSize)
	}

	if isLastChunk, err = store.AppendObjectData(orgID, objectType, objectID, dataReader, 0, startOffset, totalSize,
		isFirstChunk, isLastChunk, isTempData); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to append data for %s %s %s from offset %d to %d, Error: %s\n", orgID, objectType, objectID, startOffset, endOffset, err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return false, err
	}

	if !isLastChunk {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)

		return true, nil
	} else if isTempData {
		// Is lastChunk and need data verification:
		// send object to queue for data verification, and return
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData, send object %s %s to data verification queue\n", objectType, objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		objectForDataVerification := common.ObjectInVerifyQueue{Object: *metaData}
		objectDataVerifyQueue.SendObjectToQueue(objectForDataVerification)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData. Return response for PutObjectChunkData %s %s\n", objectType, objectID)
		}

		return true, nil
	} else {
		// It is last chunk, and no data verification is needed
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData, received last chunk for %s %s. Data verificaton is not applied\n", objectType, objectID)
		}
		// handle object info (update metadata.ObjectSize, metadata.InstanceId, metaData.DataId and object status from notReady to Ready), because Store.AppendObjectData will not modify those object info
		if _, err := store.HandleObjectInfoForLastDataChunk(orgID, objectType, objectID, false, totalSize); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, err
		}
		var updatedMetaData *common.MetaData
		// StoreObject increments the instance id if this is a data update, we need to fetch the updated meta data
		// Also, StoreObjectData updates the ObjectSize, so we need to fetch the updated meta data
		updatedMetaData, err = store.RetrieveObject(orgID, objectType, objectID)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return false, err
		}

		if updatedMetaData.Inactive {
			// Don't send inactive objects to the other side
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return true, nil
		}

		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData. Send object to objectQueue %s %s\n", objectType, objectID)
		}

		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeObject, Object: *updatedMetaData, Destinations: []common.StoreDestinationStatus{}}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In PutObjectChunkData. Return response for PutObjectChunkData %s %s\n", objectType, objectID)
		}
		return true, nil
	}
}

// ObjectConsumed is used when an app indicates that it consumed the object
// Send "consumed" notification to the object's origin
// Call the storage module to mark the object as consumed
func ObjectConsumed(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ObjectConsumed. Consumed %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as consumed. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as consumed.", orgID+":"+objectType+":"+objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to mark as consumed"}
	}

	if status != common.CompletelyReceived && status != common.ObjReceived {
		message := fmt.Sprintf("Invalid attempt to mark object in status %s as consumed\n", status)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: message}
	}

	if c, err := store.DecrementAndReturnRemainingConsumers(orgID, objectType, objectID); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in objectConsumed: failed to decrement consumers count. Error: %s\n", err)
		}
		common.ObjectLocks.Unlock(lockIndex)
	} else if c == 0 {
		if err := store.UpdateObjectStatus(orgID, objectType, objectID, common.ObjConsumed); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}

		common.ObjectLocks.Unlock(lockIndex)
		notificationsInfo, err := communications.PrepareObjectStatusNotification(*metaData, common.Consumed)

		if err != nil {
			return err
		}
		return communications.SendNotifications(notificationsInfo)
	} else {
		common.ObjectLocks.Unlock(lockIndex)
	}

	return nil
}

// ObjectPolicyReceived is called when an application wants to mark an object as having received its
// destination policy
func ObjectPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ObjectPolicyReceived. Received %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	err := store.MarkDestinationPolicyReceived(orgID, objectType, objectID)

	common.ObjectLocks.Unlock(lockIndex)

	return err
}

// ObjectReceived is called when an app indicates that it received the object
// Call the storage module to mark the object as received
func ObjectReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ObjectReceived. Received %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	status, err := store.RetrieveObjectStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as received. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as received.", orgID+":"+objectType+":"+objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to mark as received"}
	}

	if status != common.CompletelyReceived && status != common.ObjReceived {
		message := fmt.Sprintf("Invalid attempt to mark object in status %s as received\n", status)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: message}
	}

	var c int
	if c, err = store.DecrementAndReturnRemainingReceivers(orgID, objectType, objectID); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in objectReceived: failed to decrement receivers count. Error: %s\n", err)
		}
	} else if c == 0 {
		err = store.UpdateObjectStatus(orgID, objectType, objectID, common.ObjReceived)
	}

	common.ObjectLocks.Unlock(lockIndex)

	return err
}

// ObjectDeleted is called when an app indicates that 1) it deleted the object, or 2) service acknowlege service reference change
// For 1):
// Send "deleted" notification to the object's origin
// Call the storage module to delete the object if deleted by all the consumers
// For 2):
// service will be removed from ESS lastDestinationPolicyServices array
func ObjectDeleted(userID string, orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ObjectDeleted. Deleted %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("Retrieve object and status for %s %s\n", objectType, objectID)
	}
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("Status of %s %s is: %s\n", objectType, objectID, status)
	}

	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion.", orgID+":"+objectType+":"+objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to confirm deletion"}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("metaData.Deleted of %s %s is: %t\n", objectType, objectID, metaData.Deleted)
	}

	if metaData.Deleted {
		if status != common.ObjDeleted {
			message := fmt.Sprintf("Invalid attempt to confirm deletion of object in status %s\n", status)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			common.ObjectLocks.Unlock(lockIndex)
			return &common.InvalidRequest{Message: message}
		}

		if c, err := store.DecrementAndReturnRemainingConsumers(orgID, objectType, objectID); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in objectDeleted: failed to decrement consumers count. Error: %s\n", err)
			}
			common.ObjectLocks.Unlock(lockIndex)
		} else if c == 0 {
			common.ObjectLocks.Unlock(lockIndex)
			notificationsInfo, err := communications.PrepareObjectStatusNotification(*metaData, common.Deleted)
			if err != nil {
				return err
			}
			return communications.SendNotifications(notificationsInfo)
		} else {
			common.ObjectLocks.Unlock(lockIndex)
		}

	} else {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("metaData.Deleted of %s %s is: %t, get lastRemovedPolicyServices\n", objectType, objectID, metaData.Deleted)
		}

		_, lastRemovedPolicyServices, err := store.RetrieveObjectAndRemovedDestinationPolicyServices(orgID, objectType, objectID)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to find lastDestinationPolicyServices for object %s to confirm service reference change. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
			}
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}

		if len(lastRemovedPolicyServices) == 0 {
			message := fmt.Sprintln("Invalid attempt to confirm deletion of object with empty lastRemovedPolicyServices list")
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			common.ObjectLocks.Unlock(lockIndex)
			return &common.InvalidRequest{Message: message}
		}

		// only for debugging:
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("length of lastRemovedPolicyServices: %d\n", len(lastRemovedPolicyServices))
			trace.Debug("Get lastRemovedPolicyServices: \n")
			for _, s := range lastRemovedPolicyServices {
				trace.Debug("%s/%s/%s \n", s.OrgID, s.Version, s.ServiceName)
			}

			trace.Debug("Remove serviceID: %s from ESS lastRemovedPolicyServices\n", userID)
			trace.Debug("lastRemovedPolicyServices length before removal: %d\n", len(lastRemovedPolicyServices))
		}

		updatedLastRemovePolicyServices, removed := common.RemoveServiceFromServiceList(userID, lastRemovedPolicyServices)
		if !removed {
			message := fmt.Sprintln("Invalid attempt to confirm deletion of object for service not in lastRemovedPolicyServices")
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			common.ObjectLocks.Unlock(lockIndex)
			return &common.InvalidRequest{Message: message}
		}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("lastRemovedPolicyServices length after removal: %d\n", len(updatedLastRemovePolicyServices))
		}

		if err = store.UpdateRemovedDestinationPolicyServices(orgID, objectType, objectID, updatedLastRemovePolicyServices); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to update lastDestinationPolicyServices for object %s to confirm service reference change. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
			}
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}

		// only for debugging:
		if trace.IsLogging(logger.DEBUG) {
			_, lastRemovedPolicyServices, err = store.RetrieveObjectAndRemovedDestinationPolicyServices(orgID, objectType, objectID)
			trace.Debug("Get lastRemovedPolicyServices again: \n")
			if err != nil {
				trace.Debug("RetrieveObjectAndRemovedDestinationPolicyServices err: %s\n", err)
				common.ObjectLocks.Unlock(lockIndex)
			}
			for _, s := range lastRemovedPolicyServices {
				trace.Debug("%s/%s/%s \n", s.OrgID, s.Version, s.ServiceName)
			}

		}

		// keep this line
		common.ObjectLocks.Unlock(lockIndex)

	}

	return nil
}

// DeleteObject deletes an object from storage
// Call the storage module to delete the object and return the response
func DeleteObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In DeleteObject. Delete %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend && status != common.Verifying && status != common.VerificationFailed {
		// This node is not the originator of the object being deleted.
		// ESS is not allowed to remove such objects
		if common.Configuration.NodeType == common.ESS {
			common.ObjectLocks.Unlock(lockIndex)
			apiObjectLocks.Unlock(lockIndex)
			return &common.InvalidRequest{Message: "Can't delete object on the receiving side for ESS"}
		}
		// CSS removes them without notifying the other side
		err = storage.DeleteStoredObject(store, *metaData)
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	if err := storage.DeleteStoredData(store, *metaData); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	if err := store.MarkObjectDeleted(orgID, objectType, objectID); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}
	common.ObjectLocks.Unlock(lockIndex)
	apiObjectLocks.Unlock(lockIndex)

	// Notify the receivers of the object that it was deleted
	// Should be in antoher thread
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In DeleteObject. Send object to objectQueue %s %s\n", objectType, objectID)
	}

	objectInQueue := common.ObjectInQueue{NotificationAction: common.Delete, NotificationType: common.TypeObject, Object: *metaData, Destinations: []common.StoreDestinationStatus{}}
	objectQueue.SendObjectToQueue(objectInQueue)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In DeleteObject. Return response for DeleteObject %s %s\n", objectType, objectID)
	}
	return nil
}

// ActivateObject activates an inactive object
// Call the storage module to activate the object and return the response
func ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ActivateObject. Activate %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend && status != common.Verifying && status != common.VerificationFailed {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Can't activate object on the receiving side"}
	}

	if err := store.ActivateObject(orgID, objectType, objectID); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	if status == common.ReadyToSend {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In ActivateObject. Send object to objectQueue %s %s\n", objectType, objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		apiObjectLocks.Unlock(lockIndex)
		objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeObject, Object: *metaData, Destinations: []common.StoreDestinationStatus{}}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In ActivateObject. Return response for ActivateObject %s %s\n", objectType, objectID)
		}
		return nil
	}

	common.ObjectLocks.Unlock(lockIndex)
	apiObjectLocks.Unlock(lockIndex)
	return nil
}

// ListDestinations lists all destinations
func ListDestinations(orgID string) ([]common.Destination, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListDestinations.\n")
	}

	common.HealthStatus.ClientRequestReceived()

	apiLock.RLock()
	defer apiLock.RUnlock()

	return store.RetrieveDestinations(orgID, "")
}

// ResendObjects asks the other side to resend all the relevant objects
func ResendObjects() common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ResendObjects.\n")
	}

	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType == common.CSS {
		return &common.InvalidRequest{Message: "CSS can't request to resend objects"}
	}
	return communications.ResendObjects()
}

// Delete the organization
func deleteOrganization(orgID string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType == common.ESS {
		return &common.InvalidRequest{Message: "ESS can't delete organization"}
	}

	if common.SingleOrgCSS {
		return &common.InvalidRequest{Message: "Can't modify organizations for single organization CSS"}
	}

	apiLock.Lock()
	defer apiLock.Unlock()

	if err := store.DeleteOrganization(orgID); err != nil {
		return err
	}
	if err := store.DeleteOrganizationInfo(orgID); err != nil {
		return err
	}

	return communications.Comm.DeleteOrganization(orgID)
}

func updateOrganization(orgID string, org common.Organization) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	if !common.IsValidName(orgID) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Organization ID (%s) contains invalid characters", orgID)}
	}

	if common.Configuration.NodeType == common.ESS {
		return &common.InvalidRequest{Message: "ESS can't add organization"}
	}

	if common.SingleOrgCSS {
		return &common.InvalidRequest{Message: "Can't modify organizations for single organization CSS"}
	}

	if common.Configuration.CSSOnWIoTP {
		return &common.InvalidRequest{Message: "Can't modify organizations for CSS on WIoTP "}
	}

	if org.Address == "" && (common.Configuration.CommunicationProtocol == common.MQTTProtocol ||
		common.Configuration.CommunicationProtocol == common.HybridMQTT) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Can't create MQTT client for organization %s: no broker address\n", org.OrgID)}
	}

	if orgID != org.OrgID {
		return &common.InvalidRequest{Message: fmt.Sprintf("Org ID (%s) in the URL doesn't match the org-id (%s) in the payload", orgID, org.OrgID)}
	}

	if !common.IsValidName(orgID) {
		return &common.InvalidRequest{Message: fmt.Sprintf("Org ID (%s) contains invalid characters", org.OrgID)}
	}

	apiLock.Lock()
	defer apiLock.Unlock()

	timestamp, err := store.StoreOrganization(org)
	if err != nil {
		return err
	}

	return communications.Comm.UpdateOrganization(org, timestamp)
}

func getOrganizations() ([]common.Organization, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType == common.ESS {
		return nil, &common.InvalidRequest{Message: "ESS doesn't have organizations"}
	}

	apiLock.RLock()
	defer apiLock.RUnlock()

	orgs := make([]common.Organization, 0)

	if common.SingleOrgCSS {
		var protocol string
		if common.Configuration.MQTTUseSSL {
			protocol = "ssl"
		} else {
			protocol = "tcp"
		}
		address := fmt.Sprintf("%s://%s:%d", protocol, common.Configuration.BrokerAddress, common.Configuration.BrokerPort)
		org := common.Organization{OrgID: common.Configuration.OrgID, Address: address}
		orgs = append(orgs, org)
	} else {
		storedOrgs, err := store.RetrieveOrganizations()
		if err != nil {
			return nil, err
		}
		for _, storedOrg := range storedOrgs {
			orgs = append(orgs, storedOrg.Org)
		}
	}
	return orgs, nil
}

// GetObjectDestinationsStatus gets the destinations of the object and their statuses
func GetObjectDestinationsStatus(orgID string, objectType string, objectID string) ([]common.DestinationsStatus, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	dests, err := store.GetObjectDestinationsList(orgID, objectType, objectID)
	if err != nil {
		return nil, err
	}
	if len(dests) == 0 {
		return nil, nil
	}
	result := make([]common.DestinationsStatus, 0)
	for _, d := range dests {
		result = append(result, common.DestinationsStatus{DestType: d.Destination.DestType, DestID: d.Destination.DestID,
			Status: d.Status, Message: d.Message})
	}
	return result, nil
}

// GetObjectsForDestination gets objects that are in use on a given node
func GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	apiLock.RLock()
	defer apiLock.RUnlock()

	if common.Configuration.NodeType != common.CSS {
		return nil, nil
	}
	return store.GetObjectsForDestination(orgID, destType, destID)
}

// UpdateObjectDestinations updates object's destinations
func UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType != common.CSS {
		return &common.InvalidRequest{Message: "ESS doesn't support destinations update"}
	}

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)

	metaData, status, deletedDestinations, addedDestinations, err := store.UpdateObjectDestinations(orgID, objectType, objectID, destinationsList)
	if err != nil {
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	apiObjectLocks.Unlock(lockIndex)

	if len(deletedDestinations) != 0 {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObjectDestinations. Send object to objectQueue for delete%s %s\n", objectType, objectID)
		}

		objectInQueue := common.ObjectInQueue{NotificationAction: common.Delete, NotificationType: common.TypeDestination, Object: *metaData, Destinations: deletedDestinations}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObjectDestinations. Continue to add destinations\n")
		}
	}

	if len(addedDestinations) != 0 && status == common.ReadyToSend {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObjectDestinations. Send object to objectQueue for update%s %s\n", objectType, objectID)
		}

		objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeDestination, Object: *metaData, Destinations: addedDestinations}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In UpdateObjectDestinations. Continue to return response %s %s\n", objectType, objectID)
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In set destination. removed destination for object %s %s: \n", objectType, objectID)
		for _, deleted := range deletedDestinations {
			trace.Debug("%s: %s", deleted.Destination.DestType, deleted.Destination.DestID)
		}

		trace.Debug("In set destination. added destination for object %s %s: \n", objectType, objectID)
		for _, added := range addedDestinations {
			trace.Debug("%s: %s", added.Destination.DestType, added.Destination.DestID)
		}
	}
	return nil
}

// AddObjectDestinations adds destinations to object's destination list
func AddObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType != common.CSS {
		return &common.InvalidRequest{Message: "ESS doesn't support destinations update"}
	}

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)

	metaData, status, addedDestinations, err := store.AddObjectDestinations(orgID, objectType, objectID, destinationsList)
	if err != nil {
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	apiObjectLocks.Unlock(lockIndex)
	if len(addedDestinations) != 0 && status == common.ReadyToSend {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In AddObjectDestinations. Send object to objectQueue for update%s %s\n", objectType, objectID)
		}

		objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeDestination, Object: *metaData, Destinations: addedDestinations}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In AddObjectDestinations. Continue to return response %s %s\n", objectType, objectID)
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In add destinations, added destination for object %s %s: \n", objectType, objectID)
		for _, added := range addedDestinations {
			trace.Debug("%s: %s", added.Destination.DestType, added.Destination.DestID)
		}
	}
	return nil
}

// DeleteObjectDestinations deletes destinations from object's destination list
func DeleteObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	if common.Configuration.NodeType != common.CSS {
		return &common.InvalidRequest{Message: "ESS doesn't support destinations update"}
	}

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)

	metaData, _, deletedDestinations, err := store.DeleteObjectDestinations(orgID, objectType, objectID, destinationsList)
	if err != nil {
		apiObjectLocks.Unlock(lockIndex)
		return err
	}

	apiObjectLocks.Unlock(lockIndex)

	if len(deletedDestinations) != 0 {
		// Should be in antoher thread
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In DeleteObjectDestinations. Send object to objectQueue for delete%s %s\n", objectType, objectID)
		}

		objectInQueue := common.ObjectInQueue{NotificationAction: common.Delete, NotificationType: common.TypeDestination, Object: *metaData, Destinations: deletedDestinations}
		objectQueue.SendObjectToQueue(objectInQueue)

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In DeleteObjectDestinations. Continue to return response %s %s\n", objectType, objectID)
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In delete destinations. removed destination for object %s %s: \n", objectType, objectID)
		for _, deleted := range deletedDestinations {
			trace.Debug("%s: %s", deleted.Destination.DestType, deleted.Destination.DestID)
		}
	}
	return nil
}

// DeleteWebhook deletes a WebHook
func DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.DeleteWebhook(orgID, objectType, url)
}

// RegisterWebhook registers a WebHook
func RegisterWebhook(orgID string, objectType string, webhook string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	uri, err := url.Parse(webhook)
	if err != nil {
		return &common.InvalidRequest{Message: "Invalid webhook"}
	}
	if err != nil || (!strings.EqualFold(uri.Scheme, "http") && !strings.EqualFold(uri.Scheme, "https")) {
		return &common.InvalidRequest{Message: "Invalid destination data URI"}
	}

	return store.AddWebhook(orgID, objectType, webhook)
}

// AddUsersToACL adds users to an ACL.
// Note: Adding the first user to such an ACL automatically creates it.
func AddUsersToACL(aclType string, orgID string, key string, usernames []common.ACLentry) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.AddUsersToACL(aclType, orgID, key, usernames)
}

// RemoveUsersFromACL removes users from an ACL.
// Note: Removing the last user from such an ACL automatically deletes it.
func RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.RemoveUsersFromACL(aclType, orgID, key, users)
}

// RetrieveACL retrieves the list of users in the specified ACL
func RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	apiLock.RLock()
	defer apiLock.RUnlock()
	return store.RetrieveACL(aclType, orgID, key, aclUserType)
}

// RetrieveACLsInOrg retrieves the list of ACLs (object type/destination type) of the specified type in an organization
func RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.RetrieveACLsInOrg(aclType, orgID)
}
