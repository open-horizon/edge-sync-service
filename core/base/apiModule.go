package base

import (
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

	// Verify that the object is valid
	if metaData.ObjectID == "" {
		return &common.InvalidRequest{Message: "Object's meta data does not contain object's ID"}
	}
	if objectID != metaData.ObjectID {
		message := fmt.Sprintf("Object ID (%+v) in the URL doesn't match the object-id (%+v) in the payload", objectID, metaData.ObjectID)
		return &common.InvalidRequest{Message: message}
	}

	if metaData.ObjectType == "" {
		return &common.InvalidRequest{Message: "Object's meta data does not contain object's type"}
	}
	if objectType != metaData.ObjectType {
		message := fmt.Sprintf("Object type (%+v) in the URL doesn't match the object-type (%+v) in the payload", objectType, metaData.ObjectType)
		return &common.InvalidRequest{Message: message}
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

	if metaData.DestinationPolicy != nil && metaData.DestType != "" {
		return &common.InvalidRequest{Message: "Both destination policy and destination type are specified"}
	}

	if metaData.DestinationPolicy != nil && metaData.DestinationsList != nil {
		return &common.InvalidRequest{Message: "Both destination policy and destination list are specified"}
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
	}

	if metaData.DestOrgID == "" {
		metaData.DestOrgID = orgID
	}

	if metaData.ExpectedConsumers == 0 {
		metaData.ExpectedConsumers = 1
	} else if metaData.ExpectedConsumers == -1 {
		metaData.ExpectedConsumers = math.MaxInt32
	}

	// Store the object in the storage module
	status := common.NotReadyToSend
	if data != nil || metaData.Link != "" || metaData.NoData || metaData.SourceDataURI != "" {
		status = common.ReadyToSend
	} else if metaData.MetaOnly {
		reader, err := store.RetrieveObjectData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if err != nil {
			return err
		}
		if reader != nil {
			status = common.ReadyToSend
			store.CloseDataReader(reader)
		}
	}
	if metaData.NoData {
		data = nil
		metaData.Link = ""
		metaData.SourceDataURI = ""
	} else if data != nil {
		metaData.ObjectSize = int64(len(data))
	}
	metaData.ChunkSize = common.Configuration.MaxDataChunkSize

	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)
	deletedDestinations, err := store.StoreObject(metaData, data, status)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if metaData.DestinationPolicy != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return nil
	}

	store.DeleteNotificationRecords(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, "", "")

	if status == common.NotReadyToSend || metaData.Inactive {
		common.ObjectLocks.Unlock(lockIndex)
		return nil
	}

	// StoreObject increments the instance id, we need to fetch the updated meta data
	updatedMetaData, err := store.RetrieveObject(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	var deleteNotificationsInfo []common.NotificationInfo
	if len(deletedDestinations) != 0 {
		deleteNotificationsInfo, err = communications.PrepareNotificationsForDestinations(*updatedMetaData, deletedDestinations, common.Delete)
		if err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	updateNotificationsInfo, err := communications.PrepareObjectNotifications(*updatedMetaData)
	common.ObjectLocks.Unlock(lockIndex)

	if err != nil {
		return err
	}

	if deleteNotificationsInfo != nil {
		if err := communications.SendNotifications(deleteNotificationsInfo); err != nil {
			return err
		}
	}

	return communications.SendNotifications(updateNotificationsInfo)
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
func ListObjectsWithDestinationPolicyByService(orgID, arch, serviceName, version string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	apiLock.RLock()
	defer apiLock.RUnlock()

	common.HealthStatus.ClientRequestReceived()

	objects, err := store.RetrieveObjectsWithDestinationPolicyByService(orgID, arch, serviceName, version)

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ListObjectsWithDestinationPolicyByService. Get %s/%s/%s/%s. Returned %d objects\n",
			orgID, arch, serviceName, version, len(objects))
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
	if metaData == nil || status == common.NotReadyToSend || status == common.PartiallyReceived {
		return nil, nil
	}
	if metaData.DestinationDataURI != "" && status == common.CompletelyReceived {
		return dataURI.GetData(metaData.DestinationDataURI)
	}
	if metaData.SourceDataURI != "" && status == common.ReadyToSend {
		return dataURI.GetData(metaData.SourceDataURI)
	}
	return store.RetrieveObjectData(orgID, objectType, objectID)
}

// PutObjectData stores an object's data
// Call the storage module to store the object's data
// Return true if the object was found and updated
// Return false and no error if the object was not found
func PutObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In PutObjectData. Update data %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return false, err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		return false, nil
	}
	if status != common.ReadyToSend && status != common.NotReadyToSend {
		common.ObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data of the receiving side"}
	}
	if metaData.NoData {
		common.ObjectLocks.Unlock(lockIndex)
		return false, &common.InvalidRequest{Message: "Can't update data, the NoData flag is set to true"}
	}

	if exists, err := store.StoreObjectData(orgID, objectType, objectID, dataReader); err != nil || !exists {
		common.ObjectLocks.Unlock(lockIndex)
		return false, err
	}

	if metaData.SourceDataURI != "" {
		if err = store.UpdateObjectSourceDataURI(orgID, objectType, objectID, ""); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return false, err
		}
	}

	var updatedMetaData *common.MetaData
	// StoreObject increments the instance id if this is a data update, we need to fetch the updated meta data
	// Also, StoreObjectData updates the ObjectSize, so we need to fetch the updated meta data
	updatedMetaData, err = store.RetrieveObject(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return false, err
	}

	if updatedMetaData.Inactive {
		// Don't send inactive objects to the other side
		common.ObjectLocks.Unlock(lockIndex)
		return true, nil
	}

	notificationsInfo, err := communications.PrepareObjectNotifications(*updatedMetaData)
	common.ObjectLocks.Unlock(lockIndex)
	if err != nil {
		return true, err
	}
	return true, communications.SendNotifications(notificationsInfo)
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

		notificationsInfo, err := communications.PrepareObjectStatusNotification(*metaData, common.Consumed)
		common.ObjectLocks.Unlock(lockIndex)
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

// ObjectDeleted is called when an app indicates that it deleted the object
// Send "deleted" notification to the object's origin
// Call the storage module to delete the object if deleted by all the consumers
func ObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In ObjectDeleted. Deleted %s %s\n", objectType, objectID)
	}

	common.HealthStatus.ClientRequestReceived()

	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.Lock(lockIndex)
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion.", orgID+":"+objectType+":"+objectID)
		}
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Failed to find object to confirm deletion"}
	}

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
		notificationsInfo, err := communications.PrepareObjectStatusNotification(*metaData, common.Deleted)
		common.ObjectLocks.Unlock(lockIndex)
		if err != nil {
			return err
		}
		return communications.SendNotifications(notificationsInfo)
	} else {
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
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend {
		// This node is not the originator of the object being deleted.
		// ESS is not allowed to remove such objects
		if common.Configuration.NodeType == common.ESS {
			common.ObjectLocks.Unlock(lockIndex)
			return &common.InvalidRequest{Message: "Can't delete object on the receiving side for ESS"}
		}
		// CSS removes them without notifying the other side
		err = storage.DeleteStoredObject(store, *metaData)
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if err := storage.DeleteStoredData(store, *metaData); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if err := store.MarkObjectDeleted(orgID, objectType, objectID); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	// Notify the receivers of the object that it was deleted
	notificationsInfo, err := communications.PrepareDeleteNotifications(*metaData)
	common.ObjectLocks.Unlock(lockIndex)
	if err != nil {
		return err
	}
	return communications.SendNotifications(notificationsInfo)
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
	defer apiObjectLocks.Unlock(lockIndex)

	common.ObjectLocks.Lock(lockIndex)

	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}
	if metaData == nil {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend {
		common.ObjectLocks.Unlock(lockIndex)
		return &common.InvalidRequest{Message: "Can't activate object on the receiving side"}
	}

	if err := store.ActivateObject(orgID, objectType, objectID); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return err
	}

	if status == common.ReadyToSend {
		notificationsInfo, err := communications.PrepareObjectNotifications(*metaData)
		common.ObjectLocks.Unlock(lockIndex)
		if err != nil {
			return err
		}
		return communications.SendNotifications(notificationsInfo)
	}

	common.ObjectLocks.Unlock(lockIndex)
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
		return &common.InvalidRequest{Message: "Failed to update destinations: " + err.Error()}
	}

	var deleteNotificationsInfo, updateNotificationsInfo []common.NotificationInfo
	if len(deletedDestinations) != 0 {
		deleteNotificationsInfo, err = communications.PrepareNotificationsForDestinations(*metaData, deletedDestinations, common.Delete)
		if err != nil {
			apiObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	if len(addedDestinations) != 0 && status == common.ReadyToSend {
		updateNotificationsInfo, err = communications.PrepareNotificationsForDestinations(*metaData, addedDestinations, common.Update)
		if err != nil {
			apiObjectLocks.Unlock(lockIndex)
			return err
		}
	}

	apiObjectLocks.Unlock(lockIndex)
	if len(deleteNotificationsInfo) != 0 {
		if err := communications.SendNotifications(deleteNotificationsInfo); err != nil {
			return err
		}
	}
	if len(updateNotificationsInfo) != 0 {
		if err := communications.SendNotifications(updateNotificationsInfo); err != nil {
			return err
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
func AddUsersToACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.AddUsersToACL(aclType, orgID, key, usernames)
}

// RemoveUsersFromACL removes users from an ACL.
// Note: Removing the last user from such an ACL automatically deletes it.
func RemoveUsersFromACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.RemoveUsersFromACL(aclType, orgID, key, usernames)
}

// RetrieveACL retrieves the list of users in the specified ACL
func RetrieveACL(aclType string, orgID string, key string) ([]string, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	apiLock.RLock()
	defer apiLock.RUnlock()
	return store.RetrieveACL(aclType, orgID, key)
}

// RetrieveACLsInOrg retrieves the list of ACLs of the specified type in an organization
func RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	common.HealthStatus.ClientRequestReceived()

	apiLock.Lock()
	defer apiLock.Unlock()
	return store.RetrieveACLsInOrg(aclType, orgID)
}