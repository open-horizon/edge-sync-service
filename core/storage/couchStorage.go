package storage

import (
	_ "github.com/go-kivik/couchdb/v3" // The CouchDB Driver
	kivik "github.com/go-kivik/kivik/v3"

	"context"
	//"crypto/tls"
	//"crypto/x509"
	"fmt"
	"io"
	//"io/ioutil"
	//"net/http"
	//"os"
	"strings"
	"time"

	//"github.com/go-kivik/couchdb"
	"github.com/open-horizon/edge-sync-service/common"
)

// CouchStorage is a CouchDB based store
type CouchStorage struct {
	client    *kivik.Client
	loginInfo map[string]string
	dsn       string
	connected bool
}

type couchObject struct {
	ID                 string                          `json:"_id"`
	Rev                string                          `json:"_rev,omitempty"`
	MetaData           common.MetaData                 `json:"metadata"`
	Attachment         *kivik.Attachment               `json:"_attachments,omitempty"`
	Status             string                          `json:"status"`
	PolicyReceived     bool                            `json:"policy-received"`
	RemainingConsumers int                             `json:"remaining-consumers"`
	RemainingReceivers int                             `json:"remaining-receivers"`
	Destinations       []common.StoreDestinationStatus `json:"destinations"`
	LastUpdate         time.Time                       `json:"last-update"`
}

// Init initializes the CouchStorage store
func (store *CouchStorage) Init() common.SyncServiceError {

	store.loginInfo = map[string]string{
		"ipAddress": common.Configuration.CouchAddress,
		"dbName":    common.Configuration.CouchDbName,
		"Username":  common.Configuration.CouchUsername,
		"Password":  common.Configuration.CouchPassword,
	}

	store.dsn = createDSN(store.loginInfo["ipAddress"], store.loginInfo["Username"], store.loginInfo["Password"])
	var client *kivik.Client
	var err error

	// if common.Configuration.CouchUseSSL {
	// 	tlsConfig := &tls.Config{}
	// 	if common.Configuration.CouchCACertificate != "" {
	// 		var caFile string
	// 		if strings.HasPrefix(common.Configuration.CouchCACertificate, "/") {
	// 			caFile = common.Configuration.CouchCACertificate
	// 		} else {
	// 			caFile = common.Configuration.PersistenceRootPath + common.Configuration.CouchCACertificate
	// 		}
	// 		serverCaCert, err := ioutil.ReadFile(caFile)
	// 		if err != nil {
	// 			if _, ok := err.(*os.PathError); ok {
	// 				serverCaCert = []byte(common.Configuration.CouchCACertificate)
	// 				err = nil
	// 			} else {
	// 				message := fmt.Sprintf("Failed to find couch SSL CA file. Error: %s.", err)
	// 				return &Error{message}
	// 			}
	// 		}

	// 		caCertPool := x509.NewCertPool()
	// 		caCertPool.AppendCertsFromPEM(serverCaCert)
	// 		tlsConfig.RootCAs = caCertPool
	// 	}

	// 	// Please avoid using this if possible! Makes using TLS pointless
	// 	if common.Configuration.CouchAllowInvalidCertificates {
	// 		tlsConfig.InsecureSkipVerify = true
	// 	}

	// 	setXport := couchdb.SetTransport(&http.Transport{TLSClientConfig: tlsConfig})
	// 	client, err = kivik.New("couch", store.dsn)
	// 	if err != nil {
	// 		message := fmt.Sprintf("Failed to connect. Error: %s.", err)
	// 		return &Error{message}
	// 	}

	// 	err = client.Authenticate(context.TODO(), setXport)
	// 	if err != nil {
	// 		message := fmt.Sprintf("Authentication Failed. Error: %s.", err)
	// 		return &Error{message}
	// 	}
	//}

	// basicAuth := couchdb.BasicAuth(store.loginInfo["Username"], store.loginInfo["Password"])
	// err = client.Authenticate(context.TODO(), basicAuth)
	// if err != nil {
	// 	return err
	// }

	client, err = kivik.New("couch", store.dsn)
	if client == nil || err != nil {
		message := fmt.Sprintf("Failed to connect to couch. Error: %s.", err)
		return &Error{message}
	}

	store.connected = true
	store.client = client

	return nil
}

// Stop stops the CouchStorage store
func (store *CouchStorage) Stop() {
	store.client.Close(context.TODO())
}

// PerformMaintenance performs store's maintenance
func (store *CouchStorage) PerformMaintenance() {
	//store.checkObjects()
}

// Cleanup erase the on disk Bolt database only for ESS and test
func (store *CouchStorage) Cleanup(isTest bool) common.SyncServiceError {
	return nil
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *CouchStorage) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	return nil, nil
}

// StoreObject stores an object
// If the object already exists, return the changes in its destinations list (for CSS) - return the list of deleted destinations
func (store *CouchStorage) StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	id := getObjectCollectionID(metaData)
	var dests []common.StoreDestinationStatus
	var deletedDests []common.StoreDestinationStatus

	if metaData.DestinationPolicy != nil {
		metaData.DestinationPolicy.Timestamp = time.Now().UTC().UnixNano()
	}

	if status == common.NotReadyToSend || status == common.ReadyToSend {
		// The object was receieved from a service, i.e. this node is the origin of the object:
		// set its instance id and create destinations array
		newID := store.getInstanceID()
		metaData.InstanceID = newID
		if data != nil && !metaData.NoData && !metaData.MetaOnly {
			metaData.DataID = newID
		}

		var err error
		dests, deletedDests, err = createDestinationsFromMeta(store, metaData)
		if err != nil {
			return nil, err
		}
	}

	existingObject := &couchObject{}
	if err := store.getOne(id, existingObject); err != nil {
		if !strings.HasPrefix(err.Error(), "Not Found") {
			return nil, &Error{fmt.Sprintf("Failed to retrieve object's status. Error: %s.", err)}
		}
		existingObject = nil
	}

	if existingObject != nil {
		if (metaData.DestinationPolicy != nil && existingObject.MetaData.DestinationPolicy == nil) ||
			(metaData.DestinationPolicy == nil && existingObject.MetaData.DestinationPolicy != nil) {
			return nil, &common.InvalidRequest{Message: "Can't update the existence of Destination Policy"}
		}

		if metaData.MetaOnly {
			metaData.DataID = existingObject.MetaData.DataID
			metaData.ObjectSize = existingObject.MetaData.ObjectSize
			metaData.ChunkSize = existingObject.MetaData.ChunkSize
		}
		if metaData.DestinationPolicy != nil {
			dests = existingObject.Destinations
		}

	}

	newObject := couchObject{ID: id, MetaData: metaData, Status: status, PolicyReceived: false,
		RemainingConsumers: metaData.ExpectedConsumers,
		RemainingReceivers: metaData.ExpectedConsumers, Destinations: dests}

	if existingObject != nil {
		if err := store.upsert(newObject, true); err != nil {
			return nil, &Error{fmt.Sprintf("Failed to store an object. Error: %s.", err)}
		}
	} else {
		if err := store.upsert(newObject, false); err != nil {
			return nil, &Error{fmt.Sprintf("Failed to store an object. Error: %s.", err)}
		}
	}

	if !metaData.NoData && data != nil {
		if err := store.addAttachment(id, data); err != nil {
			return nil, err
		}
	} else if !metaData.MetaOnly {
		if err := store.removeAttachment(id); err != nil {
			return nil, err
		}
	}

	return deletedDests, nil
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *CouchStorage) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)

	if err := store.getOne(id, &result); err != nil {
		if !strings.HasPrefix(err.Error(), "Not Found") {
			return nil, "", &Error{fmt.Sprintf("Failed to fetch the object. Error: %s.", err)}
		}

		return nil, "", nil
	}
	return &result.MetaData, result.Status, nil
}

// DeleteStoredObject deletes the object
func (store *CouchStorage) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	//	id := createObjectCollectionID(orgID, objectType, objectID)
	//	return store.deleteObject(id)
	return nil
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *CouchStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	dests := make([]common.Destination, 0)

	return dests, nil
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *CouchStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {

	return nil, nil
}

// UpdateObjectDestinations updates object's destinations
// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
func (store *CouchStorage) UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {

	return nil, "", nil, nil, nil
}

// UpdateObjectDeliveryStatus changes the object's delivery status and message for the destination
// Returns true if the status is Deleted and all the destinations are in status Deleted
func (store *CouchStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) (bool, common.SyncServiceError) {

	return false, nil
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *CouchStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	return nil
}

// RetrieveObjectStatus finds the object and return its status
func (store *CouchStorage) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {

	return "", nil
}

// RetrieveObjectRemainingConsumers finds the object and returns the number remaining consumers that
// haven't consumed the object yet
func (store *CouchStorage) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {

	return 0, nil
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *CouchStorage) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {

	return 0, nil
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *CouchStorage) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {

	return 0, nil
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *CouchStorage) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {

	return nil
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed or received
// If received is true, return objects marked as received
func (store *CouchStorage) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {

	return nil, nil
}

// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
// If received is true, return objects marked as policy received
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {

	return nil, nil
}

// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {

	return nil, nil
}

// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {

	return nil, nil
}

// RetrieveObjectsWithFilters returns the list of all the objects that meet the given conditions
func (store *CouchStorage) RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string) ([]common.MetaData, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination.
// Adds the new destination to the destinations lists of the relevant objects.
func (store *CouchStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	return nil, nil
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
// ESS only API
func (store *CouchStorage) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *CouchStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *CouchStorage) RetrieveObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	return nil, nil
}

// CloseDataReader closes the data reader if necessary
func (store *CouchStorage) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	return nil
}

// ReadObjectData returns the object data with the specified parameters
func (store *CouchStorage) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {

	return nil, false, 0, nil
}

// StoreObjectData stores object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *CouchStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {

	return true, nil
}

// AppendObjectData appends a chunk of data to the object's data
func (store *CouchStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader,
	dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError {

	return nil
}

// UpdateObjectStatus updates object's status
func (store *CouchStorage) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {

	return nil
}

// UpdateObjectSourceDataURI updates object's source data URI
func (store *CouchStorage) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	return nil
}

// MarkObjectDeleted marks the object as deleted
func (store *CouchStorage) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {

	return nil
}

// MarkDestinationPolicyReceived marks an object's destination policy as having been received
func (store *CouchStorage) MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {

	return nil
}

// ActivateObject marks object as active
func (store *CouchStorage) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {

	return nil
}

// DeleteStoredData deletes the object's data
func (store *CouchStorage) DeleteStoredData(orgID string, objectType string, objectID string) common.SyncServiceError {

	return nil
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *CouchStorage) CleanObjects() common.SyncServiceError {
	// ESS only function
	return nil
}

// GetNumberOfStoredObjects returns the number of objects received from the application that are
// currently stored in this node's storage
func (store *CouchStorage) GetNumberOfStoredObjects() (uint32, common.SyncServiceError) {
	return 0, nil
}

// AddWebhook stores a webhook for an object type
func (store *CouchStorage) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	return nil
}

// DeleteWebhook deletes a webhook for an object type
func (store *CouchStorage) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	return nil
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *CouchStorage) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *CouchStorage) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	return nil, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *CouchStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {

	return true, nil
}

// StoreDestination stores the destination
func (store *CouchStorage) StoreDestination(destination common.Destination) common.SyncServiceError {

	return nil
}

// DeleteDestination deletes the destination
func (store *CouchStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {

	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *CouchStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {

	return nil
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *CouchStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {

}

// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
func (store *CouchStorage) GetNumberOfDestinations() (uint32, common.SyncServiceError) {
	return 0, nil
}

// RetrieveDestinationProtocol retrieves the communication protocol for the destination
func (store *CouchStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	return "", nil
}

// RetrieveAllObjects retrieves All Objects
func (store *CouchStorage) RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	return nil, nil
}

// RetrieveDestination retrieves a destination
func (store *CouchStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	return nil, nil
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *CouchStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	return nil, nil
}

// RetrieveAllObjectsAndUpdateDestinationListForDestination retrieves objects that are in use on a given node and the destination status
func (store *CouchStorage) RetrieveAllObjectsAndUpdateDestinationListForDestination(destOrgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
func (store *CouchStorage) RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError) {
	return nil, nil, nil
}

// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
func (store *CouchStorage) UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError {
	return nil
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *CouchStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {

	return nil
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *CouchStorage) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {

	return nil
}

// RetrieveNotificationRecord retrieves notification
func (store *CouchStorage) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {

	return nil, nil
}

// DeleteNotificationRecords deletes notification records to an object
func (store *CouchStorage) DeleteNotificationRecords(orgID string, objectType string, objectID string, destType string, destID string) common.SyncServiceError {

	return nil
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *CouchStorage) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError) {

	return nil, nil
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *CouchStorage) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	return nil, nil
}

// InsertInitialLeader inserts the initial leader document if the collection is empty
func (store *CouchStorage) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {

	return true, nil
}

// LeaderPeriodicUpdate does the periodic update of the leader document by the leader
func (store *CouchStorage) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {

	return true, nil
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *CouchStorage) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	return "", 0, time.Time{}, 0, nil
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *CouchStorage) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {

	return true, nil
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *CouchStorage) ResignLeadership(leaderID string) common.SyncServiceError {

	return nil
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *CouchStorage) RetrieveTimeOnServer() (time.Time, error) {
	return time.Now(), nil
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *CouchStorage) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {

	return nil
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *CouchStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {

	return nil
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *CouchStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {

	return "", nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *CouchStorage) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup,
	common.SyncServiceError) {

	return nil, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *CouchStorage) DeleteOrganization(orgID string) common.SyncServiceError {

	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *CouchStorage) IsConnected() bool {
	return store.connected
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *CouchStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {

	return time.Time{}, nil
}

// RetrieveOrganizationInfo retrieves organization information
func (store *CouchStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *CouchStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {

	return nil
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *CouchStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *CouchStorage) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// AddUsersToACL adds users to an ACL
func (store *CouchStorage) AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return nil
}

// RemoveUsersFromACL removes users from an ACL
func (store *CouchStorage) RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return nil
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *CouchStorage) RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	return nil, nil
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *CouchStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
func (store *CouchStorage) RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *CouchStorage) IsPersistent() bool {
	return true
}
