package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

// InMemoryStorage is an in-memory store
type InMemoryStorage struct {
	lockChannel   chan int
	objects       map[string]inMemoryObject
	notifications map[string]common.Notification
	webhooks      map[string][]string
	timebase      int64
}

type inMemoryObject struct {
	meta               common.MetaData
	data               []byte
	status             string
	remainingConsumers int
	remainingReceivers int
	consumedTimestamp  time.Time
}

// Init initializes the InMemory store
func (store *InMemoryStorage) Init() common.SyncServiceError {
	store.lockChannel = make(chan int, 1)
	store.lockChannel <- 1
	store.objects = make(map[string]inMemoryObject)
	store.notifications = make(map[string]common.Notification)
	store.webhooks = make(map[string][]string)

	currentTime := time.Now().UnixNano()
	store.timebase = currentTime
	currentTimeInSeconds := currentTime / 1e9
	persistedTimeBase := currentTimeInSeconds

	dir := common.Configuration.PersistenceRootPath + "/sync/local/"
	err := os.MkdirAll(dir, 0755)
	if err == nil {
		path := dir + "persisted-data"
		persisted := store.readPersistedTimebase(path)
		if persisted != 0 && currentTimeInSeconds <= persisted {
			persistedTimeBase++
			store.timebase = persistedTimeBase * 1e9
		}
		if err := store.writePersistedTimebase(path, persistedTimeBase); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Fatal("Failed to persist timebase. Error: %s\n", err.Error())
			}
		}
	}
	return nil
}

// Stop stops the InMemory store
func (store *InMemoryStorage) Stop() {
	// The InMemory store doesn't have to do anything at shutdown time.
	// This empty implementation insures that it implements the storage interface
}

// PerformMaintenance performs store's maintenance
func (store *InMemoryStorage) PerformMaintenance() {
}

// StoreObject stores an object
func (store *InMemoryStorage) StoreObject(metaData common.MetaData, data []byte, status string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := getObjectCollectionID(metaData)
	// If the object was receieved from a service (status NotReadyToSend/ReadyToSend), i.e. this node is the origin of the object,
	// set instance id. If the object was received from the other side, this node is the receiver of the object:
	// keep the instance id of the meta data.
	if status == common.NotReadyToSend || status == common.ReadyToSend {
		metaData.InstanceID = store.getInstanceID()
	}

	if metaData.MetaOnly {
		if object, ok := store.objects[id]; ok {
			if object.status == common.ConsumedByDest {
				// On ESS we remove the data of consumed objects, therefore we can't accept "meta only" updates
				return &Error{"Can't update only the meta data of consumed object"}
			}
			object.meta = metaData
			object.status = status
			object.remainingConsumers = metaData.ExpectedConsumers
			object.remainingReceivers = metaData.ExpectedConsumers
			if metaData.NoData {
				object.data = nil
			}
			store.objects[id] = object
		} else {
			return &Error{"Failed to find object to update its meta data"}
		}
	} else {
		if metaData.NoData {
			data = nil
		}
		store.objects[id] = inMemoryObject{meta: metaData, data: data, status: status,
			remainingConsumers: metaData.ExpectedConsumers, remainingReceivers: metaData.ExpectedConsumers}
	}
	return nil
}

// StoreObjectData stores an object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *InMemoryStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	var data []byte
	var err error
	if data, err = ioutil.ReadAll(dataReader); err != nil {
		return false, err
	}

	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		if object.status == common.NotReadyToSend {
			object.status = common.ReadyToSend
		} else if object.status == common.ReadyToSend {
			object.meta.InstanceID = store.getInstanceID()
		}
		object.data = data
		object.meta.ObjectSize = int64(len(object.data))
		store.objects[id] = object
		return true, nil
	}

	return false, nil
}

// AppendObjectData appends a chunk of data to the object's data
func (store *InMemoryStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader, dataLength uint32,
	offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	object, ok := store.objects[id]
	if ok {
		var data []byte
		if dataLength == 0 {
			dt, err := ioutil.ReadAll(dataReader)
			if err != nil {
				return &Error{"Failed to read object data. Error: " + err.Error()}
			}
			data = dt
			dataLength = uint32(len(data))
		}
		if total < offset+int64(dataLength) {
			total = offset + int64(dataLength)
		}
		if isFirstChunk {
			object.data = make([]byte, total)
		} else {
			object.data = ensureArrayCapacity(object.data, total)
		}
		if data != nil {
			copy(object.data[offset:], data)
		} else {
			count, err := dataReader.Read(object.data[offset:])
			if err != nil {
				return &Error{"Failed to read object data. Error: " + err.Error()}
			}
			if count != int(dataLength) {
				return &Error{fmt.Sprintf("Read %d bytes for the object data, instead of %d", count, dataLength)}
			}
		}
		store.objects[id] = object
		return nil
	}

	return notFound
}

// UpdateObjectStatus updates an object's status
func (store *InMemoryStorage) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.status = status
		if status == common.ConsumedByDest {
			object.consumedTimestamp = time.Now()
		}
		store.objects[id] = object
		return nil
	}

	return &NotFound{"Object not found"}
}

// UpdateObjectSourceDataURI updates object's source data URI
func (store *InMemoryStorage) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.meta.SourceDataURI = sourceDataURI
		store.objects[id] = object
		return nil
	}

	return notFound
}

// RetrieveObjectStatus finds the object and returns its status
func (store *InMemoryStorage) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		return object.status, nil
	}

	return "", nil
}

// RetrieveObjectRemainingConsumers finds the object and returns the number of remaining consumers
// that haven't consumed the object yet
func (store *InMemoryStorage) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		return object.remainingConsumers, nil
	}

	return 0, notFound
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *InMemoryStorage) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.remainingConsumers = object.meta.ExpectedConsumers
		store.objects[id] = object
		return nil
	}

	return notFound
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *InMemoryStorage) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.remainingConsumers--
		store.objects[id] = object
		return object.remainingConsumers, nil
	}

	return 0, notFound
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *InMemoryStorage) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.remainingReceivers--
		store.objects[id] = object
		return object.remainingReceivers, nil
	}

	return 0, notFound
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed or received
// If received is true, return objects marked as received
func (store *InMemoryStorage) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	result := make([]common.MetaData, 0)
	for _, obj := range store.objects {
		if objectType == obj.meta.ObjectType &&
			(obj.status == common.CompletelyReceived || obj.status == common.ObjDeleted ||
				(obj.status == common.ObjReceived && received)) {
			result = append(result, obj.meta)
		}
	}
	return result, nil
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination
func (store *InMemoryStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	result := make([]common.MetaData, 0)
	for _, obj := range store.objects {
		if !obj.meta.Inactive && obj.status == common.ReadyToSend &&
			(obj.meta.DestType == "" || obj.meta.DestType == destType || destType == "") &&
			(obj.meta.DestID == "" || obj.meta.DestID == destID || destID == "") {
			result = append(result, obj.meta)
		}
	}
	return result, nil
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
func (store *InMemoryStorage) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	result := make([]common.ConsumedObject, 0)
	for _, obj := range store.objects {
		if obj.status == common.ConsumedByDest {
			result = append(result, common.ConsumedObject{MetaData: obj.meta, Timestamp: obj.consumedTimestamp})
		}
	}
	return result, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *InMemoryStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		return &object.meta, nil
	}

	return nil, nil
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *InMemoryStorage) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		return &object.meta, object.status, nil
	}

	return nil, "", nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *InMemoryStorage) RetrieveObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		if object.data != nil && len(object.data) > 0 {
			return bytes.NewReader(object.data), nil
		}
		return nil, nil
	}

	return nil, nil
}

// CloseDataReader closes the data reader if necessary
func (store *InMemoryStorage) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	switch v := dataReader.(type) {
	case *os.File:
		return v.Close()
	}
	return nil
}

// ReadObjectData returns the object data with the specified parameters
func (store *InMemoryStorage) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		lod := int64(len(object.data))
		if lod <= offset {
			return make([]byte, 0), true, 0, nil
		}
		s := int64(size)
		eof := false
		if s >= lod-offset {
			s = lod - offset
			eof = true
		}
		b := make([]byte, s)
		copy(b, object.data[offset:])
		return b, eof, int(s), nil
	}

	return nil, true, 0, &common.NotFound{}
}

// MarkObjectDeleted marks the object as deleted
func (store *InMemoryStorage) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.meta.Deleted = true
		object.status = common.ObjDeleted
		store.objects[id] = object
		return nil
	}

	return notFound
}

// ActivateObject marks object as active
func (store *InMemoryStorage) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.meta.Inactive = false
		store.objects[id] = object
		return nil
	}

	return notFound
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *InMemoryStorage) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	currentTime := time.Now().Format(time.RFC3339)
	result := make([]common.MetaData, 0)
	for _, obj := range store.objects {
		if (obj.status == common.NotReadyToSend || obj.status == common.ReadyToSend) &&
			obj.meta.Inactive && obj.meta.ActivationTime != "" && obj.meta.ActivationTime <= currentTime {
			result = append(result, obj.meta)
		}
	}
	return result, nil
}

// DeleteStoredObject deletes the object
func (store *InMemoryStorage) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	delete(store.objects, id)
	return nil
}

// DeleteStoredData deletes the object's data
func (store *InMemoryStorage) DeleteStoredData(orgID string, objectType string, objectID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := createObjectCollectionID(orgID, objectType, objectID)
	if object, ok := store.objects[id]; ok {
		object.data = nil
		store.objects[id] = object
		return nil
	}

	return notFound
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *InMemoryStorage) CleanObjects() common.SyncServiceError {
	store.lock()
	defer store.unLock()

	for _, obj := range store.objects {
		if obj.status == common.PartiallyReceived || obj.status == common.CompletelyReceived {
			id := createObjectCollectionID(obj.meta.DestOrgID, obj.meta.ObjectType, obj.meta.ObjectID)
			delete(store.objects, id)
		}
	}
	return nil
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *InMemoryStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	return []common.Destination{common.Destination{DestOrgID: metaData.DestOrgID, DestType: common.Configuration.DestinationType,
		DestID: common.Configuration.DestinationID, Communication: common.Configuration.CommunicationProtocol}}, nil
}

// UpdateObjectDeliveryStatus changes the object's delivery status for the destination
func (store *InMemoryStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) common.SyncServiceError {
	return nil
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *InMemoryStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	return nil
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *InMemoryStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	return nil, nil
}

// AddWebhook stores a webhook for an object type
func (store *InMemoryStorage) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	var hooks []string
	if h := store.webhooks[objectType]; h != nil {
		hooks = h
	} else {
		hooks = make([]string, 0)
	}

	// Don't add the webhook if it already is in the list
	for _, hook := range hooks {
		if url == hook {
			return nil
		}
	}

	hooks = append(hooks, url)
	store.webhooks[objectType] = hooks

	return nil
}

// DeleteWebhook deletes a webhook for an object type
func (store *InMemoryStorage) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	if hooks := store.webhooks[objectType]; hooks != nil {
		for i, hook := range hooks {
			if strings.EqualFold(hook, url) {
				hooks[i] = hooks[len(hooks)-1]
				store.webhooks[objectType] = hooks[:len(hooks)-1]
				return nil
			}
		}
	}

	return nil
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *InMemoryStorage) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	store.lock()
	defer store.unLock()
	if hooks := store.webhooks[objectType]; hooks != nil {
		if len(hooks) == 0 {
			return nil, &NotFound{"No webhooks"}
		}
		return hooks, nil
	}
	return nil, &NotFound{"No webhooks"}
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *InMemoryStorage) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	return nil, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *InMemoryStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	return true, nil
}

// StoreDestination stores a destination
func (store *InMemoryStorage) StoreDestination(destination common.Destination) common.SyncServiceError {
	return nil
}

// DeleteDestination deletes a destination
func (store *InMemoryStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *InMemoryStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	return nil
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *InMemoryStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {}

// RetrieveDestination retrieves a destination
func (store *InMemoryStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	return &common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID,
		Communication: common.Configuration.CommunicationProtocol}, nil
}

// RetrieveDestinationProtocol retrieves communication protocol for the destination
func (store *InMemoryStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	return common.Configuration.CommunicationProtocol, nil
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *InMemoryStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	return nil, nil
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *InMemoryStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	notification.ResendTime = time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
	id := getNotificationCollectionID(&notification)
	store.notifications[id] = notification
	return nil
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *InMemoryStorage) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	id := getNotificationCollectionID(&notification)
	if notification, ok := store.notifications[id]; ok {
		resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
		notification.ResendTime = resendTime
		store.notifications[id] = notification
		return nil
	}
	return notFound
}

// RetrieveNotificationRecord retrieves notification
func (store *InMemoryStorage) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
	if notification, ok := store.notifications[id]; ok {
		return &notification, nil
	}

	return nil, nil
}

// DeleteNotificationRecords deletes notification records to an object
func (store *InMemoryStorage) DeleteNotificationRecords(orgID string, objectType string, objectID string,
	destType string, destID string) common.SyncServiceError {
	store.lock()
	defer store.unLock()

	if objectType != "" && objectID != "" {
		if destType != "" && destID != "" {
			id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
			delete(store.notifications, id)
		} else {
			for id, notification := range store.notifications {
				if notification.ObjectType == objectType && notification.ObjectID == objectID {
					delete(store.notifications, id)
				}
			}
		}
	} else {
		for id, notification := range store.notifications {
			if (notification.DestType == destType || destType == "") && (notification.DestID == destID || destID == "") {
				delete(store.notifications, id)
			}
		}
	}
	return nil
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *InMemoryStorage) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification,
	common.SyncServiceError) {
	store.lock()
	defer store.unLock()

	result := make([]common.Notification, 0)
	if destID != "" && destType != "" {
		for _, notification := range store.notifications {
			if notification.DestType == destType && notification.DestID == destID && resendNotification(notification, false) {
				result = append(result, notification)
			}
		}
	} else {
		currentTime := time.Now().Unix()
		for _, notification := range store.notifications {
			if resendNotification(notification, false) {
				if notification.ResendTime <= currentTime || notification.Status == common.Getdata {
					result = append(result, notification)
				}
			}
		}
	}
	return result, nil
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *InMemoryStorage) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	return nil, nil
}

// InsertInitialLeader inserts the initial leader entry
func (store *InMemoryStorage) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {
	return true, nil
}

// LeaderPeriodicUpdate does the periodic update of the leader entry by the leader
func (store *InMemoryStorage) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {
	return false, nil
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *InMemoryStorage) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	return "", 0, time.Now(), 0, nil
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *InMemoryStorage) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {
	return false, nil
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *InMemoryStorage) ResignLeadership(leaderID string) common.SyncServiceError {
	return nil
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *InMemoryStorage) RetrieveTimeOnServer() (time.Time, error) {
	return time.Now(), nil
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *InMemoryStorage) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {
	return nil
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *InMemoryStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	return nil
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *InMemoryStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	return "", nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *InMemoryStorage) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup, common.SyncServiceError) {
	return nil, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *InMemoryStorage) DeleteOrganization(orgID string) common.SyncServiceError {
	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *InMemoryStorage) IsConnected() bool {
	return true
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *InMemoryStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {
	return time.Now(), nil
}

// RetrieveOrganizationInfo retrieves organization information
func (store *InMemoryStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *InMemoryStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	return nil
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *InMemoryStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *InMemoryStorage) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// AddUsersToACL adds users to an ACL
func (store *InMemoryStorage) AddUsersToACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return nil
}

// RemoveUsersFromACL removes users from an ACL
func (store *InMemoryStorage) RemoveUsersFromACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return nil
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *InMemoryStorage) RetrieveACL(aclType string, orgID string, key string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *InMemoryStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return nil, nil
}

func (store *InMemoryStorage) getInstanceID() int64 {
	// Always called from inside the lock - no need to lock here
	store.timebase++
	return store.timebase
}

func (store *InMemoryStorage) lock() {
	<-store.lockChannel
}

func (store *InMemoryStorage) unLock() {
	store.lockChannel <- 1
}

const (
	timebaseType = iota
)

func (store *InMemoryStorage) readPersistedTimebase(path string) int64 {
	if _, err := os.Stat(path); err != nil {
		return 0
	}

	data, err := dataURI.GetData("file://" + path)
	if err != nil || data == nil {
		return 0
	}
	var (
		magicValue   uint32
		versionMajor uint32
		versionMinor uint32
		fieldCount   uint32
		fieldType    uint32
		fieldLength  uint32
		timebase     int64
	)

	if err = binary.Read(data, binary.BigEndian, &magicValue); err != nil {
		return 0
	}
	if magicValue != common.Magic {
		return 0
	}

	if err = binary.Read(data, binary.BigEndian, &versionMajor); err != nil {
		return 0
	}

	if versionMajor != common.Version.Major {
		return 0
	}

	if err = binary.Read(data, binary.BigEndian, &versionMinor); err != nil {
		return 0
	}

	if versionMinor != common.Version.Minor {
		return 0
	}

	if err = binary.Read(data, binary.BigEndian, &fieldCount); err != nil {
		return 0
	}

	for i := 0; i < int(fieldCount); i++ {
		if err = binary.Read(data, binary.BigEndian, &fieldType); err != nil {
			return 0
		}
		if err = binary.Read(data, binary.BigEndian, &fieldLength); err != nil {
			return 0
		}

		switch int(fieldType) {
		case timebaseType:
			if fieldLength != uint32(binary.Size(timebase)) {
				return 0
			}
			if err = binary.Read(data, binary.BigEndian, &timebase); err != nil {
				return 0
			}

		default:
			return 0
		}
	}
	return timebase
}

func (store *InMemoryStorage) writePersistedTimebase(path string, timebase int64) common.SyncServiceError {
	message := new(bytes.Buffer)

	// magic
	var value = common.Magic
	err := binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}

	// version
	value = common.Version.Major
	err = binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}
	value = common.Version.Minor
	err = binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}

	// fieldCount
	value = 1
	err = binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}

	// field type
	value = timebaseType
	err = binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}

	// length
	value = uint32(binary.Size(timebase))
	err = binary.Write(message, binary.BigEndian, value)
	if err != nil {
		return err
	}

	// timebase
	if err = binary.Write(message, binary.BigEndian, timebase); err != nil {
		return err
	}

	_, err = dataURI.StoreData("file://"+path, message, 0)
	return err
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *InMemoryStorage) IsPersistent() bool {
	return false
}
