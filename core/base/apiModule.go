package base

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

// app sends an updated object
func updateObject(orgID string, objectType string, objectID string, metaData common.MetaData, data []byte) common.SyncServiceError {

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
		expiration, err := time.Parse(time.RFC3339, metaData.Expiration)
		if err != nil {
			return &common.InvalidRequest{Message: "Failed to parse expiration in object's meta data. Error: " + err.Error()}
		}
		if time.Now().After(expiration) {
			return &common.InvalidRequest{Message: "Invalid expiration time in object's meta data"}
		}
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
	if data != nil || metaData.Link != "" || metaData.NoData || metaData.MetaOnly || metaData.SourceDataURI != "" {
		status = common.ReadyToSend
	}
	if metaData.NoData {
		data = nil
		metaData.Link = ""
		metaData.SourceDataURI = ""
	} else if data != nil {
		metaData.ObjectSize = int64(len(data))
	}
	metaData.ChunkSize = common.Configuration.MaxDataChunkSize

	if err := store.StoreObject(metaData, data, status); err != nil {
		return err
	}

	store.DeleteNotificationRecords(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, "", "")

	if status == common.NotReadyToSend || metaData.Inactive {
		return nil
	}

	// StoreObject increments the instance id, we need to fetch the updated meta data
	updatedMetaData, err := store.RetrieveObject(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	if err != nil {
		return err
	}

	return communications.SendObjectNotifications(*updatedMetaData)
}

// send the status of the object to the app
// Call the storage module to get the status of the object and return it in the response
func getObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	return store.RetrieveObjectStatus(orgID, objectType, objectID)
}

// provide a list of edge updated objects
// Call the storage module to get the list of edge updated objects and send it to the app
func listUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	return store.RetrieveUpdatedObjects(orgID, objectType, received)
}

// deliver an object to the app
// Call the storage module to get the object's meta data and send it to the app
func getObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	return store.RetrieveObject(orgID, objectType, objectID)
}

// deliver an object data to the app
// Call the storage module to get the object's data and send it to the app
func getObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
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

// store object's data
// Call the storage module to store the object's data
// Return true if the object was found and updated
// Return false and no error if the object was not found
func putObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		return false, err
	}
	if metaData == nil {
		return false, nil
	}
	if status != common.ReadyToSend && status != common.NotReadyToSend {
		return false, &common.InvalidRequest{Message: "Can't update data of the receiving side"}
	}
	if metaData.NoData {
		return false, &common.InvalidRequest{Message: "Can't update data, the NoData flag is set to true"}
	}

	if exists, err := store.StoreObjectData(orgID, objectType, objectID, dataReader); err != nil || !exists {
		return false, err
	}

	if metaData.SourceDataURI != "" {
		if err = store.UpdateObjectSourceDataURI(orgID, objectType, objectID, ""); err != nil {
			return false, err
		}
	}

	var updatedMetaData *common.MetaData
	// StoreObject increments the instance id if this is a data update, we need to fetch the updated meta data
	// Also, StoreObjectData updates the ObjectSize, so we need to fetch the updated meta data
	updatedMetaData, err = store.RetrieveObject(orgID, objectType, objectID)
	if err != nil {
		return false, err
	}

	return true, communications.SendObjectNotifications(*updatedMetaData)
}

// app indicates that it consumed the object
// Send "consumed" notification to the object's origin
// Call the storage module to mark the object as consumed
func objectConsumed(orgID string, objectType string, objectID string) common.SyncServiceError {
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as consumed. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as consumed.", orgID+":"+objectType+":"+objectID)
		}
		return &common.InvalidRequest{Message: "Failed to find object to mark as consumed"}
	}

	if status != common.CompletelyReceived && status != common.ObjReceived {
		message := fmt.Sprintf("Invalid attempt to mark object in status %s as consumed\n", status)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return &common.InvalidRequest{Message: message}
	}

	if c, err := store.DecrementAndReturnRemainingConsumers(orgID, objectType, objectID); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in objectConsumed: failed to decrement consumers count. Error: %s\n", err)
		}
	} else if c == 0 {
		if err := store.UpdateObjectStatus(orgID, objectType, objectID, common.ObjConsumed); err != nil {
			return err
		}
		if err := communications.SendObjectStatus(*metaData, common.Consumed); err != nil {
			return err
		}
	}

	return nil
}

// app indicates that it received the object
// Call the storage module to mark the object as received
func objectReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	status, err := store.RetrieveObjectStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as received. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to mark as received.", orgID+":"+objectType+":"+objectID)
		}
		return &common.InvalidRequest{Message: "Failed to find object to mark as received"}
	}

	if status != common.CompletelyReceived && status != common.ObjReceived {
		message := fmt.Sprintf("Invalid attempt to mark object in status %s as received\n", status)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return &common.InvalidRequest{Message: message}
	}

	if c, err := store.DecrementAndReturnRemainingReceivers(orgID, objectType, objectID); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in objectReceived: failed to decrement receivers count. Error: %s\n", err)
		}
	} else if c == 0 {
		if err := store.UpdateObjectStatus(orgID, objectType, objectID, common.ObjReceived); err != nil {
			return err
		}
	}

	return nil
}

// app indicates that it deleted the object
// Send "deleted" notification to the object's origin
// Call the storage module to delete the object if deleted by all the consumers
func objectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion. Error: %s", orgID+":"+objectType+":"+objectID, err.Error())
		}
		return err
	}
	if status == "" {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to find object %s to confirm deletion.", orgID+":"+objectType+":"+objectID)
		}
		return &common.InvalidRequest{Message: "Failed to find object to confirm deletion"}
	}

	if status != common.ObjDeleted {
		message := fmt.Sprintf("Invalid attempt to confirm deletion of object in status %s\n", status)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return &common.InvalidRequest{Message: message}
	}

	if c, err := store.DecrementAndReturnRemainingConsumers(orgID, objectType, objectID); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in objectDeleted: failed to decrement consumers count. Error: %s\n", err)
		}
	} else if c == 0 {
		if err := communications.SendObjectStatus(*metaData, common.Deleted); err != nil {
			return err
		}
	}

	return nil
}

// delete an object from storage
// Call the storage module to delete the object and return the response
func deleteObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		return err
	}
	if metaData == nil {
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend {
		// This node is not the originator of the object being deleted.
		// ESS is not allowed to remove such objects
		if common.Configuration.NodeType == common.ESS {
			return &common.InvalidRequest{Message: "Can't delete object on the receiving side for ESS"}
		}
		// CSS removes them without notifying the other side
		return store.DeleteStoredObject(orgID, objectType, objectID)
	}

	if err := store.DeleteStoredData(orgID, objectType, objectID); err != nil {
		return err
	}

	if err := store.MarkObjectDeleted(orgID, objectType, objectID); err != nil {
		return err
	}

	// Notify the receivers of the object that it was deleted
	return communications.SendDeleteNotifications(*metaData)
}

// Activate an inactive object
// Call the storage module to activate the object and return the response
func activateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	metaData, status, err := store.RetrieveObjectAndStatus(orgID, objectType, objectID)
	if err != nil {
		return err
	}
	if metaData == nil {
		return &common.InvalidRequest{Message: "Object not found"}
	}
	if status != common.NotReadyToSend && status != common.ReadyToSend {
		return &common.InvalidRequest{Message: "Can't activate object on the receiving side"}
	}

	if err := store.ActivateObject(orgID, objectType, objectID); err != nil {
		return err
	}

	if status == common.ReadyToSend {
		return communications.SendObjectNotifications(*metaData)
	}
	return nil
}

// list all destinations
func listDestinations(orgID string) ([]common.Destination, common.SyncServiceError) {
	return store.RetrieveDestinations(orgID, "")
}

// Ask the other side to resend all the relevant objects
func resendObjects() common.SyncServiceError {
	if common.Configuration.NodeType == common.CSS {
		return &common.InvalidRequest{Message: "CSS can't request to resend objects"}
	}
	return communications.ResendObjects()
}

// Delete the organization
func deleteOrganization(orgID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return &common.InvalidRequest{Message: "ESS can't delete organization"}
	}
	if common.SingleOrgCSS {
		return &common.InvalidRequest{Message: "Can't modify organizations for single organization CSS"}
	}

	if err := store.DeleteOrganization(orgID); err != nil {
		return err
	}
	if err := store.DeleteOrganizationInfo(orgID); err != nil {
		return err
	}

	return communications.Comm.DeleteOrganization(orgID)
}

func updateOrganization(orgID string, org common.Organization) common.SyncServiceError {
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

	timestamp, err := store.StoreOrganization(org)
	if err != nil {
		return err
	}

	return communications.Comm.UpdateOrganization(org, timestamp)
}

func getOrganizations() ([]common.Organization, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, &common.InvalidRequest{Message: "ESS doesn't have organizations"}
	}

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

// Get the destinations of the object and their statuses
func getObjectDestinationsStatus(orgID string, objectType string, objectID string) ([]common.DestinationsStatus, common.SyncServiceError) {
	dests, err := store.GetObjectDestinationsList(orgID, objectType, objectID)
	if err != nil {
		return nil, err
	}
	if len(dests) == 0 {
		return nil, nil
	}
	result := make([]common.DestinationsStatus, 0)
	for _, d := range dests {
		result = append(result, common.DestinationsStatus{DestType: d.Destination.DestType, DestID: d.Destination.DestID, Status: d.Status})
	}
	return result, nil
}

func deleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	return store.DeleteWebhook(orgID, objectType, url)
}

func registerWebhook(orgID string, objectType string, webhook string) common.SyncServiceError {
	uri, err := url.Parse(webhook)
	if err != nil {
		return &common.InvalidRequest{Message: "Invalid webhook"}
	}
	if err != nil || (!strings.EqualFold(uri.Scheme, "http") && !strings.EqualFold(uri.Scheme, "https")) {
		return &common.InvalidRequest{Message: "Invalid destination data URI"}
	}

	return store.AddWebhook(orgID, objectType, webhook)
}

func addUsersToACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return store.AddUsersToACL(aclType, orgID, key, usernames)
}

func removeUsersFromACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return store.RemoveUsersFromACL(aclType, orgID, key, usernames)
}

func retrieveACL(aclType string, orgID string, key string) ([]string, common.SyncServiceError) {
	return store.RetrieveACL(aclType, orgID, key)
}

func retrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return store.RetrieveACLsInOrg(aclType, orgID)
}
