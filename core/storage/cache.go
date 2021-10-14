package storage

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

// Cache is the caching store
type Cache struct {
	destinations map[string]map[string]common.Destination
	Store        Storage
	lock         sync.RWMutex
}

// Init initializes the Cache store
func (store *Cache) Init() common.SyncServiceError {
	if err := store.Store.Init(); err != nil {
		return err
	}

	return store.cacheDestinations()
}

func (store *Cache) cacheDestinations() common.SyncServiceError {
	destinations, err := store.Store.RetrieveDestinations("", "")
	if err != nil {
		return &Error{"Failed to initialize the cache. Error: " + err.Error()}
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	store.destinations = make(map[string]map[string]common.Destination, 0)
	for _, dest := range destinations {
		if store.destinations[dest.DestOrgID] == nil {
			store.destinations[dest.DestOrgID] = make(map[string]common.Destination, 0)
		}
		id := dest.DestType + ":" + dest.DestID
		store.destinations[dest.DestOrgID][id] = dest
	}
	return nil
}

// Stop stops the Cache store
func (store *Cache) Stop() {
	store.Store.Stop()
}

// PerformMaintenance performs store's maintenance
func (store *Cache) PerformMaintenance() {
	store.Store.PerformMaintenance()
}

// Cleanup erase the on disk Bolt database only for ESS and test
func (store *Cache) Cleanup(isTest bool) common.SyncServiceError {
	return store.Store.Cleanup(isTest)
}

// StoreObject stores an object
func (store *Cache) StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	return store.Store.StoreObject(metaData, data, status)
}

// StoreObjectData stores an object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *Cache) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	return store.Store.StoreObjectData(orgID, objectType, objectID, dataReader)
}

func (store *Cache) StoreObjectTempData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	return store.Store.StoreObjectTempData(orgID, objectType, objectID, dataReader)
}

func (store *Cache) RemoveObjectTempData(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.RemoveObjectTempData(orgID, objectType, objectID)
}

func (store *Cache) RetrieveObjectTempData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	return store.Store.RetrieveObjectTempData(orgID, objectType, objectID)
}

// AppendObjectData appends a chunk of data to the object's data
func (store *Cache) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader, dataLength uint32,
	offset int64, total int64, isFirstChunk bool, isLastChunk bool, isTempData bool) (bool, common.SyncServiceError) {
	return store.Store.AppendObjectData(orgID, objectType, objectID, dataReader, dataLength, offset, total, isFirstChunk, isLastChunk, isTempData)
}

func (store *Cache) HandleLastDataChunk(orgID string, objectType string, objectID string, isTempData bool) common.SyncServiceError {
	return store.Store.HandleLastDataChunk(orgID, objectType, objectID, isTempData)
}

// UpdateObjectStatus updates an object's status
func (store *Cache) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {
	return store.Store.UpdateObjectStatus(orgID, objectType, objectID, status)
}

// UpdateObjectDataVerifiedStatus updates object's dataVerified field
func (store *Cache) UpdateObjectDataVerifiedStatus(orgID string, objectType string, objectID string, verified bool) common.SyncServiceError {
	return store.Store.UpdateObjectDataVerifiedStatus(orgID, objectType, objectID, verified)
}

// UpdateObjectSourceDataURI pdates object's source data URI
func (store *Cache) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	return store.Store.UpdateObjectSourceDataURI(orgID, objectType, objectID, sourceDataURI)
}

// RetrieveObjectStatus finds the object and return its status
func (store *Cache) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	return store.Store.RetrieveObjectStatus(orgID, objectType, objectID)
}

// RetrieveObjectRemainingConsumers finds the object and returns the number of remaining consumers
// that haven't consumed the object yet
func (store *Cache) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {
	return store.Store.RetrieveObjectRemainingConsumers(orgID, objectType, objectID)
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *Cache) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	return store.Store.DecrementAndReturnRemainingConsumers(orgID, objectType, objectID)
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *Cache) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	return store.Store.DecrementAndReturnRemainingReceivers(orgID, objectType, objectID)
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *Cache) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.ResetObjectRemainingConsumers(orgID, objectType, objectID)
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed or received
// If received is true, return objects marked as received
func (store *Cache) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	return store.Store.RetrieveUpdatedObjects(orgID, objectType, received)
}

// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
// If received is true, return objects marked as policy received
func (store *Cache) RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	return store.Store.RetrieveObjectsWithDestinationPolicy(orgID, received)
}

// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
func (store *Cache) RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	return store.Store.RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName)
}

// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
func (store *Cache) RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	return store.Store.RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID, since)
}

// RetrieveObjectsWithFilters returns the list of all othe objects that meet the given conditions
func (store *Cache) RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string, deleted *bool) ([]common.MetaData, common.SyncServiceError) {
	return store.Store.RetrieveObjectsWithFilters(orgID, destinationPolicy, dpServiceOrgID, dpServiceName, dpPropertyName, since, objectType, objectID, destinationType, destinationID, noData, expirationTimeBefore, deleted)
}

// RetrieveAllObjects returns the list of all the objects of the specified type
func (store *Cache) RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	return store.Store.RetrieveAllObjects(orgID, objectType)
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination
func (store *Cache) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	return store.Store.RetrieveObjects(orgID, destType, destID, resend)
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
func (store *Cache) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	return store.Store.RetrieveConsumedObjects()
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *Cache) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	return store.Store.RetrieveObject(orgID, objectType, objectID)
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *Cache) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	return store.Store.RetrieveObjectAndStatus(orgID, objectType, objectID)
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *Cache) RetrieveObjectData(orgID string, objectType string, objectID string, isTempData bool) (io.Reader, common.SyncServiceError) {
	return store.Store.RetrieveObjectData(orgID, objectType, objectID, isTempData)
}

// ReadObjectData returns the object data with the specified parameters
func (store *Cache) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	return store.Store.ReadObjectData(orgID, objectType, objectID, size, offset)
}

// CloseDataReader closes the data reader if necessary
func (store *Cache) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	return store.Store.CloseDataReader(dataReader)
}

// // Get the data size
// func (store *Cache) GetObjectDataSize(orgID string, objectType string, objectID string, isTempData bool) (int64, common.SyncServiceError) {
// 	return store.Store.GetObjectDataSize(orgID, objectType, objectID, isTempData)
// }

// MarkObjectDeleted marks the object as deleted
func (store *Cache) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.MarkObjectDeleted(orgID, objectType, objectID)
}

// MarkDestinationPolicyReceived marks an object's destination policy as having been received
func (store *Cache) MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.MarkDestinationPolicyReceived(orgID, objectType, objectID)
}

// ActivateObject marks object as active
func (store *Cache) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.ActivateObject(orgID, objectType, objectID)
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *Cache) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	return store.Store.GetObjectsToActivate()
}

// DeleteStoredObject deletes the object
func (store *Cache) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.DeleteStoredObject(orgID, objectType, objectID)
}

// DeleteStoredData deletes the object's data
func (store *Cache) DeleteStoredData(orgID string, objectType string, objectID string, isTempData bool) common.SyncServiceError {
	return store.Store.DeleteStoredData(orgID, objectType, objectID, isTempData)
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *Cache) CleanObjects() common.SyncServiceError {
	return store.Store.CleanObjects()
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *Cache) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	return store.Store.GetObjectDestinations(metaData)
}

// UpdateObjectDeliveryStatus changes the object's delivery status for the destination
// Returns true if the status is Deleted and all the destinations are in status Deleted
func (store *Cache) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) (bool, common.SyncServiceError) {
	return store.Store.UpdateObjectDeliveryStatus(status, message, orgID, objectType, objectID, destType, destID)
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *Cache) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	return store.Store.UpdateObjectDelivering(orgID, objectType, objectID)
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *Cache) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	return store.Store.GetObjectDestinationsList(orgID, objectType, objectID)
}

// UpdateObjectDestinations updates object's destinations
// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
func (store *Cache) UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	return store.Store.UpdateObjectDestinations(orgID, objectType, objectID, destinationsList)
}

// AddObjectdestinations adds the destinations to object's destination list
// Returns the metadata, object's status, an array of added destinations after removing the overlapped destinations
func (store *Cache) AddObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, common.SyncServiceError) {
	return store.Store.AddObjectDestinations(orgID, objectType, objectID, destinationsList)
}

// DeleteObjectdestinations deletes the destinations from object's destination list
// Returns the metadata, objects' status, an array of destinations that removed from the current destination list
func (store *Cache) DeleteObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, common.SyncServiceError) {
	return store.Store.DeleteObjectDestinations(orgID, objectType, objectID, destinationsList)
}

// GetNumberOfStoredObjects returns the number of objects received from the application that are
// currently stored in this node's storage
func (store *Cache) GetNumberOfStoredObjects() (uint32, common.SyncServiceError) {
	return store.Store.GetNumberOfStoredObjects()
}

// AddWebhook stores a webhook for an object type
func (store *Cache) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	return store.Store.AddWebhook(orgID, objectType, url)
}

// DeleteWebhook deletes a webhook for an object type
func (store *Cache) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	return store.Store.DeleteWebhook(orgID, objectType, url)
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *Cache) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	return store.Store.RetrieveWebhooks(orgID, objectType)
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *Cache) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	store.lock.RLock()
	defer store.lock.RUnlock()

	result := make([]common.Destination, 0)
	if orgID == "" {
		for _, orgDests := range store.destinations {
			for _, value := range orgDests {
				if destType == "" || value.DestType == destType {
					result = append(result, value)
				}
			}
		}
	} else {
		for _, value := range store.destinations[orgID] {
			if destType == "" || value.DestType == destType {
				result = append(result, value)
			}
		}
	}
	return result, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *Cache) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	store.lock.RLock()
	defer store.lock.RUnlock()

	if _, ok := store.destinations[orgID][destType+":"+destID]; ok {
		return true, nil
	}
	return false, nil
}

// StoreDestination stores the destination
func (store *Cache) StoreDestination(dest common.Destination) common.SyncServiceError {
	if err := store.Store.StoreDestination(dest); err != nil {
		return err
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	if store.destinations[dest.DestOrgID] == nil {
		store.destinations[dest.DestOrgID] = make(map[string]common.Destination, 0)
	}
	store.destinations[dest.DestOrgID][dest.DestType+":"+dest.DestID] = dest
	return nil
}

// DeleteDestination deletes the destination
func (store *Cache) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	if err := store.Store.DeleteDestination(orgID, destType, destID); err != nil {
		return err
	}

	store.lock.Lock()
	defer store.lock.Unlock()

	delete(store.destinations[orgID], destType+":"+destID)
	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *Cache) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	return store.Store.UpdateDestinationLastPingTime(destination) // ???
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *Cache) RemoveInactiveDestinations(lastTimestamp time.Time) {
	store.Store.RemoveInactiveDestinations(lastTimestamp)
	store.cacheDestinations()
}

// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
func (store *Cache) GetNumberOfDestinations() (uint32, common.SyncServiceError) {
	return uint32(len(store.destinations)), nil
}

// RetrieveDestination retrieves a destination
func (store *Cache) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	store.lock.RLock()
	defer store.lock.RUnlock()

	if d, ok := store.destinations[orgID][destType+":"+destID]; ok {
		return &d, nil
	}
	return nil, &Error{fmt.Sprintf("Destination %s not found.", orgID+":"+destType+":"+destID)}
}

// RetrieveDestinationProtocol retrieves the communication protocol for the destination
func (store *Cache) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	store.lock.RLock()
	defer store.lock.RUnlock()

	if d, ok := store.destinations[orgID][destType+":"+destID]; ok {
		return d.Communication, nil
	}
	return "", &Error{fmt.Sprintf("Destination %s not found.", orgID+":"+destType+":"+destID)}
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *Cache) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	return store.Store.GetObjectsForDestination(orgID, destType, destID)
}

// RetrieveAllObjectsAndUpdateDestinationListForDestination retrieves objects that are in use on a given node and returns the list of metadata
func (store *Cache) RetrieveAllObjectsAndUpdateDestinationListForDestination(orgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError) {
	return store.Store.RetrieveAllObjectsAndUpdateDestinationListForDestination(orgID, destType, destID)
}

// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
func (store *Cache) RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError) {
	return store.Store.RetrieveObjectAndRemovedDestinationPolicyServices(orgID, objectType, objectID)
}

// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
func (store *Cache) UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError {
	return store.Store.UpdateRemovedDestinationPolicyServices(orgID, objectType, objectID, destinationPolicyServices)
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *Cache) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	return store.Store.UpdateNotificationRecord(notification)
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *Cache) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {
	return store.Store.UpdateNotificationResendTime(notification)
}

// RetrieveNotificationRecord retrieves notification
func (store *Cache) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {
	return store.Store.RetrieveNotificationRecord(orgID, objectType, objectID, destType, destID)
}

// DeleteNotificationRecords deletes notification records to an object
func (store *Cache) DeleteNotificationRecords(orgID string, objectType string, objectID string,
	destType string, destID string) common.SyncServiceError {
	return store.Store.DeleteNotificationRecords(orgID, objectType, objectID, destType, destID)
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *Cache) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError) {
	return store.Store.RetrieveNotifications(orgID, destType, destID, retrieveReceived)
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *Cache) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	return store.Store.RetrievePendingNotifications(orgID, destType, destID)
}

// InsertInitialLeader inserts the initial leader entry
func (store *Cache) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {
	return store.Store.InsertInitialLeader(leaderID)
}

// LeaderPeriodicUpdate does the periodic update of the leader entry by the leader
func (store *Cache) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {
	return store.Store.LeaderPeriodicUpdate(leaderID)
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *Cache) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	return store.Store.RetrieveLeader()
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *Cache) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {
	return store.Store.UpdateLeader(leaderID, version)
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *Cache) ResignLeadership(leaderID string) common.SyncServiceError {
	return store.Store.ResignLeadership(leaderID)
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *Cache) RetrieveTimeOnServer() (time.Time, error) {
	return store.Store.RetrieveTimeOnServer()
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *Cache) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {
	return store.Store.StoreOrgToMessagingGroup(orgID, messagingGroup)
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *Cache) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	return store.Store.DeleteOrgToMessagingGroup(orgID)
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *Cache) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	return store.Store.RetrieveMessagingGroup(orgID)
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *Cache) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup, common.SyncServiceError) {
	return store.Store.RetrieveUpdatedMessagingGroups(time)
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *Cache) DeleteOrganization(orgID string) common.SyncServiceError {
	delete(store.destinations, orgID)

	return store.Store.DeleteOrganization(orgID)
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *Cache) IsConnected() bool {
	return store.Store.IsConnected()
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *Cache) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {
	return store.Store.StoreOrganization(org)
}

// RetrieveOrganizationInfo retrieves organization information
func (store *Cache) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	return store.Store.RetrieveOrganizationInfo(orgID)
}

// DeleteOrganizationInfo deletes organization information
func (store *Cache) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	return store.Store.DeleteOrganizationInfo(orgID)
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *Cache) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	return store.Store.RetrieveOrganizations()
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *Cache) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	return store.Store.RetrieveUpdatedOrganizations(time)
}

// AddUsersToACL adds users to an ACL
func (store *Cache) AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.Store.AddUsersToACL(aclType, orgID, key, users)
}

// RemoveUsersFromACL removes users from an ACL
func (store *Cache) RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.Store.RemoveUsersFromACL(aclType, orgID, key, users)
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *Cache) RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	return store.Store.RetrieveACL(aclType, orgID, key, aclUserType)
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *Cache) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return store.Store.RetrieveACLsInOrg(aclType, orgID)
}

// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
func (store *Cache) RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *Cache) IsPersistent() bool {
	return store.Store.IsPersistent()
}
