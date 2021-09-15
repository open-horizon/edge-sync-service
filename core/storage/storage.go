package storage

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
)

const (
	destinations    = "syncDestinations"
	leader          = "syncLeaderElection"
	notifications   = "syncNotifications"
	objects         = "syncObjects"
	messagingGroups = "syncMessagingGroups"
	webhooks        = "syncWebhooks"
	organizations   = "syncOrganizations"
	acls            = "syncACLs"
)

// Storage is the interface for stores
type Storage interface {
	// Initialize the store
	Init() common.SyncServiceError

	// Stop the store
	Stop()

	// PerformMaintenance performs store's maintenance
	PerformMaintenance()

	// Cleanup erase the on disk Bolt databass only for ESS and test
	Cleanup(isTest bool) common.SyncServiceError

	// Store an object
	// If the object already exists, return the changes in its destinations list (for CSS) - return the list of deleted destinations
	StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError)

	// Store object's data
	// Return true if the object was found and updated
	// Return false and no error, if the object doesn't exist
	StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError)

	StoreObjectTempData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError)

	RemoveObjectTempData(orgID string, objectType string, objectID string) common.SyncServiceError

	RetrieveTempObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError)

	// Append a chunk of data to the object's data
	AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader, dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError

	// Update object's status
	UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError

	// Update object's source data URI
	UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError

	// Find the object and return its status
	RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError)

	// Find the object and return the number of remaining consumers that haven't consumed the object yet
	RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError)

	// Decrement the number of remaining consumers of the object
	DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError)

	// Decrement the number of remaining receivers of the object
	DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int, common.SyncServiceError)

	// Sets the remaining consumers count to the original ExpectedConsumers value
	ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError

	// Return the list of all the edge updated objects that are not marked as consumed or received
	// If received is true, return objects marked as received
	RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError)

	// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
	// If received is true, return objects marked as policy received
	RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError)

	// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
	RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError)

	// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
	RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError)

	// RetrieveObjectsWithFilters returns the list of all othe objects that meet the given conditions
	RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string, deleted *bool) ([]common.MetaData, common.SyncServiceError)

	// RetrieveAllObjects returns the list of all the objects of the specified type
	RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError)

	// Return the list of all the objects that need to be sent to the destination
	RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError)

	// RetrieveConsumedObjects returns all the consumed objects originated from this node
	RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError)

	// Return the object meta data with the specified parameters
	RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError)

	// Return the object meta data and status with the specified parameters
	RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError)

	// Return the object data with the specified parameters
	RetrieveObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError)

	// Return the object data with the specified parameters
	ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError)

	// Close the data reader if necessary
	CloseDataReader(dataReader io.Reader) common.SyncServiceError

	// Marks the object as deleted
	MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError

	// Mark an object's destination policy as having been received
	MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError

	// Mark object as active
	ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError

	// GetObjectsToActivate returns inactive objects that are ready to be activated
	GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError)

	// Delete the object
	DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError

	// Delete the object's data
	DeleteStoredData(orgID string, objectType string, objectID string) common.SyncServiceError

	// CleanObjects removes the objects received from the other side.
	// For persistant storage only partially recieved objects are removed.
	CleanObjects() common.SyncServiceError

	// Get destinations that the object has to be sent to
	GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError)

	// UpdateObjectDeliveryStatus changes the object's delivery status for the destination
	// Returns true if the status is Deleted and all the destinations are in status Deleted
	UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
		destType string, destID string) (bool, common.SyncServiceError)

	// UpdateObjectDelivering marks the object as being delivered to all its destinations
	UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError

	// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
	GetObjectDestinationsList(orgID string, objectType string,
		objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError)

	// UpdateObjectDestinations updates object's destinations
	// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
	UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
		[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError)

	// AddObjectdestinations adds the destinations to object's destination list
	// Returns the metadata, object's status, an array of added destinations after removing the overlapped destinations
	AddObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
		[]common.StoreDestinationStatus, common.SyncServiceError)

	// DeleteObjectdestinations deletes the destinations from object's destination list
	// Returns the metadata, objects' status, an array of destinations that removed from the current destination list
	DeleteObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
		[]common.StoreDestinationStatus, common.SyncServiceError)

	// GetNumberOfStoredObjects returns the number of objects received from the application that are
	// currently stored in this node's storage
	GetNumberOfStoredObjects() (uint32, common.SyncServiceError)

	// AddWebhook stores a webhook for an object type
	AddWebhook(orgID string, objectType string, url string) common.SyncServiceError

	// DeleteWebhook deletes a webhook for an object type
	DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError

	// RetrieveWebhooks gets the webhooks for the object type
	RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError)

	// Return all the destinations with the provided orgID and destType
	RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError)

	// Return true if the destination exists, and false otherwise
	DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError)

	// Retrieve destination
	RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError)

	// Store the destination
	StoreDestination(destination common.Destination) common.SyncServiceError

	// Delete the destination
	DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError

	// UpdateDestinationLastPingTime updates the last ping time for the destination
	UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError

	// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
	RemoveInactiveDestinations(lastTimestamp time.Time)

	// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
	GetNumberOfDestinations() (uint32, common.SyncServiceError)

	// Retrieve communication protocol for the destination
	RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError)

	// GetObjectsForDestination retrieves objects that are in use on a given node
	GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError)

	// RetrieveAllObjectsAndUpdateDestinationListForDestination retrieves objects that are in use on a given node and returns the list of metadata
	RetrieveAllObjectsAndUpdateDestinationListForDestination(orgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError)

	// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
	RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError)

	// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
	UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError

	// Update/add a notification record to an object
	UpdateNotificationRecord(notification common.Notification) common.SyncServiceError

	// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
	UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError

	// RetrieveNotificationRecord retrieves notification
	RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string, destID string) (*common.Notification, common.SyncServiceError)

	// Delete notification records to an object
	DeleteNotificationRecords(orgID string, objectType string, objectID string, destType string, destID string) common.SyncServiceError

	// Return the list of all the notifications that need to be resent to the destination
	RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError)

	// Return the list of pending notifications that are waiting to be sent to the destination
	RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError)

	// InsertInitialLeader inserts the initial leader document in the collection is empty
	InsertInitialLeader(leaderID string) (bool, common.SyncServiceError)

	// LeaderPeriodicUpdate does the periodic update of the leader document by the leader
	LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError)

	// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
	RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError)

	// UpdateLeader updates the leader entry for a leadership takeover
	UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError)

	// ResignLeadership causes this sync service to give up the Leadership
	ResignLeadership(leaderID string) common.SyncServiceError

	// RetrieveTimeOnServer retrieves the current time on the database server
	RetrieveTimeOnServer() (time.Time, error)

	// StoreOrgToMessagingGroup inserts organization to messaging groups table
	StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError

	// DeleteOrgToMessagingGroup deletes organization from messaging groups table
	DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError

	// RetrieveMessagingGroup retrieves messaging group for organization
	RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError)

	// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
	RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup, common.SyncServiceError)

	// DeleteOrganization cleans up the storage from all the records associated with the organization
	DeleteOrganization(orgID string) common.SyncServiceError

	// StoreOrganization stores organization information
	// Returns the stored record timestamp for multiple CSS updates
	StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError)

	// RetrieveOrganizationInfo retrieves organization information
	RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError)

	// DeleteOrganizationInfo deletes organization information
	DeleteOrganizationInfo(orgID string) common.SyncServiceError

	// RetrieveOrganizations retrieves stored organizations' info
	RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError)

	// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
	RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError)

	// AddUsersToACL adds users to an ACL
	AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError

	// RemoveUsersFromACL removes users from an ACL
	RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError

	// RetrieveACL retrieves the list of usernames on an ACL
	RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError)

	// RetrieveACLsInOrg retrieves the list of ACLs in an organization
	RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError)

	// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
	RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError)

	// IsConnected returns false if the storage cannont be reached, and true otherwise
	IsConnected() bool

	// IsPersistent returns true if the storage is persistent, and false otherwise
	IsPersistent() bool
}

// Error is the error used in the storage layer
type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// NotFound is the error returned if an object wasn't found
type NotFound struct {
	message string
}

func (e *NotFound) Error() string {
	return e.message
}

// IsNotFound returns true if the error passed in is the storage.NotFound error
func IsNotFound(err error) bool {
	_, ok := err.(*NotFound)
	return ok
}

var notFound = &NotFound{"Object not found"}

// NotConnected is the error returned if there is no connection to the database
type NotConnected struct {
	message string
}

func (e *NotConnected) Error() string {
	return e.message
}

// IsNotConnected returns true if the error passed in is the storage.NotConnected error
func IsNotConnected(err error) bool {
	_, ok := err.(*NotConnected)
	return ok
}

// Discarded is the error returned if an out-of-order chunk wasn't appended to the stored object because of memory usage protection
type Discarded struct {
	message string
}

func (e *Discarded) Error() string {
	return e.message
}

// IsDiscarded returns true if the error passed in is the storage.Discarded error
func IsDiscarded(err error) bool {
	_, ok := err.(*Discarded)
	return ok
}

// Objects
func getObjectCollectionID(metaData common.MetaData) string {
	return createObjectCollectionID(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
}

func createObjectCollectionID(orgID string, objectType string, objectID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(orgID) + len(objectType) + len(objectID) + 3)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectID)
	return strBuilder.String()
}

func createTempObjectCollectionID(orgID string, objectType string, objectID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(orgID) + len(objectType) + len(objectID) + len("tmp") + 4)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString("tmp")
	return strBuilder.String()
}

// Notifications
func getNotificationCollectionID(notification *common.Notification) string {
	return createNotificationCollectionID(notification.DestOrgID, notification.ObjectType, notification.ObjectID, notification.DestType,
		notification.DestID)
}

func createNotificationCollectionID(orgID string, objectType string, objectID string, destType string, destID string) string {
	return common.CreateNotificationID(orgID, objectType, objectID, destType, destID)
}

// Destinations
func getDestinationCollectionID(destination common.Destination) string {
	return createDestinationCollectionID(destination.DestOrgID, destination.DestType, destination.DestID)
}

func createDestinationCollectionID(orgID string, destType string, destID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(orgID) + len(destType) + len(destID) + 3)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(destType)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(destID)
	return strBuilder.String()
}

func resendNotification(notification common.Notification, retrieveReceived bool) bool {
	s := notification.Status
	return (s == common.Update || s == common.Consumed || s == common.Getdata || s == common.Delete || s == common.Deleted || s == common.Received || s == common.Error ||
		(retrieveReceived && (s == common.Data || s == common.ReceivedByDestination)))
}

func ensureArrayCapacity(data []byte, newCapacity int64) []byte {
	if newCapacity <= int64(cap(data)) {
		return data
	}
	new := make([]byte, newCapacity)
	copy(new, data)
	return new
}

func createDataPath(prefix string, orgID string, objectType string, objectID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(prefix) + len(orgID) + len(objectType) + len(objectID) + 3)
	strBuilder.WriteString(prefix)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('-')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte('-')
	strBuilder.WriteString(objectID)
	return strBuilder.String()
}

func createDataPathForTempData(prefix string, orgID string, objectType string, objectID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(prefix) + len(orgID) + len(objectType) + len(objectID) + len("tmp") + 4)
	strBuilder.WriteString(prefix)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte('-')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte('-')
	strBuilder.WriteString(objectID)
	strBuilder.WriteByte('-')
	strBuilder.WriteString("tmp")
	return strBuilder.String()
}

func createDataPathFromMeta(prefix string, metaData common.MetaData) string {
	return createDataPath(prefix, metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
}

func createDestinationFromList(orgID string, store Storage, destinationsList []string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	dests := make([]common.StoreDestinationStatus, 0)
	for _, d := range destinationsList {
		parts := strings.Split(d, ":")
		if len(parts) == 2 {
			if dest, err := store.RetrieveDestination(orgID, parts[0], parts[1]); err == nil && dest != nil {
				dests = append(dests, common.StoreDestinationStatus{Destination: *dest, Status: common.Pending})
			} else {
				if IsNotFound(err) {
					return nil, &common.InvalidRequest{Message: fmt.Sprintf("Invalid destination %s:%s", parts[0], parts[1])}
				}
				return nil, &Error{fmt.Sprintf("Failed to find destination %s:%s", parts[0], parts[1])}
			}
		} else {
			return nil, &common.InvalidRequest{Message: fmt.Sprintf("Invalid destination %s", d)}
		}
	}
	return dests, nil
}

func compareDestinations(oldList []common.StoreDestinationStatus, newList []common.StoreDestinationStatus, useOldStatus bool) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus, []common.StoreDestinationStatus) {
	deletedDests := make([]common.StoreDestinationStatus, 0)
	addedDests := make([]common.StoreDestinationStatus, 0)
	for _, dest := range oldList {
		found := false
		for index, newDest := range newList {
			if dest.Destination == newDest.Destination {
				if useOldStatus {
					newList[index] = dest
				}
				found = true
				break
			}
		}
		if !found {
			deletedDests = append(deletedDests, dest)
		}
	}
	for index, newDest := range newList {
		found := false
		for _, dest := range oldList {
			if dest.Destination == newDest.Destination {
				if useOldStatus {
					newList[index] = dest
				}
				found = true
				break
			}
		}
		if !found {
			addedDests = append(addedDests, newDest)
		}
	}
	return newList, deletedDests, addedDests
}

func getDestinationsForAdd(orgID string, store Storage, currentDestinationList []common.StoreDestinationStatus, destinationsListToAdd []string) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	destsToAdd, err := createDestinationFromList(orgID, store, destinationsListToAdd)
	if err != nil {
		return nil, nil, err
	}
	updatedDests, addedDests := compareDestinationsForAdd(currentDestinationList, destsToAdd)
	return updatedDests, addedDests, nil
}

func getDestinationsForDelete(orgID string, store Storage, currentDestinationList []common.StoreDestinationStatus, destinationsListToDelete []string) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	destsToDelete, err := createDestinationFromList(orgID, store, destinationsListToDelete)
	if err != nil {
		return nil, nil, err
	}
	updatedDests, deletedDests := compareDestinationsForDelete(currentDestinationList, destsToDelete)
	return updatedDests, deletedDests, nil
}

func compareDestinationsForAdd(currDests []common.StoreDestinationStatus, destToAdd []common.StoreDestinationStatus) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus) {
	// ADD:
	// 1. get the list of destinations need to add (remove form list if destination is already in currentDests)
	// 2. return addedDests => need to send notification
	// 3. get the new list of dests, set it in db
	addedDests := make([]common.StoreDestinationStatus, 0)
	for _, dest := range destToAdd {
		found := false
		for _, currDest := range currDests {
			if dest.Destination == currDest.Destination {
				// update destToAdd because destination status is initially "pending", need to use the existing destination status
				found = true
				break
			}
		}
		if !found {
			addedDests = append(addedDests, dest)
		}
	}

	updatedDests := append(currDests, addedDests...)

	return updatedDests, addedDests
}

func compareDestinationsForDelete(currDests []common.StoreDestinationStatus, destToDelete []common.StoreDestinationStatus) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus) {
	// REMOVE:
	// 1. get the list of destinations need to remove (remove from list if destination is not in currentDests)
	// 2. return deletedDests => need to send notification
	// 3. get the new list of dests, set it in db
	deletedDests := make([]common.StoreDestinationStatus, 0)
	updatedDests := make([]common.StoreDestinationStatus, 0)
	indexesToRmFromCurr := make(map[int]struct{}, 0)
	for _, dest := range destToDelete {
		for idx, currDest := range currDests {
			if dest.Destination == currDest.Destination {
				// dest to delete is found in current dests, then add it to deletedDests
				deletedDests = append(deletedDests, currDest)
				indexesToRmFromCurr[idx] = struct{}{}
				break
			}
		}
	}

	// remove deletedDests from currentDests
	for currIdx, currDest := range currDests {
		if _, ok := indexesToRmFromCurr[currIdx]; !ok {
			// only add dest to updatedDests if the dest is not in deletedDests
			updatedDests = append(updatedDests, currDest)
		}
	}

	return updatedDests, deletedDests
}

func createDestinationsFromMeta(store Storage, metaData common.MetaData) ([]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	if metaData.DestinationPolicy != nil {
		return nil, nil, nil
	}
	dests := make([]common.StoreDestinationStatus, 0)
	if metaData.DestID != "" {
		// We check that destType is not empty in updateObject()
		if dest, err := store.RetrieveDestination(metaData.DestOrgID, metaData.DestType, metaData.DestID); err == nil && dest != nil {
			dests = append(dests, common.StoreDestinationStatus{Destination: *dest, Status: common.Pending})
		}
	} else {
		if len(metaData.DestinationsList) == 0 {
			// Either broadcast or destType without destID
			if destinations, err := store.RetrieveDestinations(metaData.DestOrgID, metaData.DestType); err == nil {
				for _, dest := range destinations {
					dests = append(dests, common.StoreDestinationStatus{Destination: dest, Status: common.Pending})
				}
			}
		} else {
			var err error
			dests, err = createDestinationFromList(metaData.DestOrgID, store, metaData.DestinationsList)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	existingDestList, _ := store.GetObjectDestinationsList(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	if existingDestList != nil {
		dests, deletedDests, _ := compareDestinations(existingDestList, dests, false)
		return dests, deletedDests, nil
	}

	return dests, nil, nil
}

func createDestinations(orgID string, store Storage, existingDestinations []common.StoreDestinationStatus, destinationsList []string) ([]common.StoreDestinationStatus,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {

	dests, err := createDestinationFromList(orgID, store, destinationsList)
	if err != nil {
		return nil, nil, nil, err
	}

	dests, deletedDests, addedDests := compareDestinations(existingDestinations, dests, true)
	return dests, deletedDests, addedDests, nil
}

// DeleteStoredObject calls the storage to delete the object and its data
func DeleteStoredObject(store Storage, metaData common.MetaData) common.SyncServiceError {
	if err := store.DeleteStoredObject(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID); err != nil {
		return err
	}

	if common.Configuration.NodeType == common.ESS && metaData.DestinationDataURI != "" {
		if err := dataURI.DeleteStoredData(metaData.DestinationDataURI); err != nil {
			return err
		}
	}

	return nil
}

// DeleteStoredData calls the storage to delete the object's data
func DeleteStoredData(store Storage, metaData common.MetaData) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS && metaData.DestinationDataURI != "" {
		if err := dataURI.DeleteStoredData(metaData.DestinationDataURI); err != nil {
			return err
		}
		return nil
	}

	return store.DeleteStoredData(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
}
