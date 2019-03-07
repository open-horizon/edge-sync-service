package storage

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"

	bolt "github.com/etcd-io/bbolt"
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
)

const timebaseBucketName = "syncTimebase"

// BoltStorage is a Bolt based store
type BoltStorage struct {
	db            *bolt.DB
	timebase      int64
	lockChannel   chan int
	localDataPath string
}

type boltObject struct {
	Meta               common.MetaData `json:"meta"`
	Status             string          `json:"status"`
	RemainingConsumers int             `json:"remaining-consumers"`
	RemainingReceivers int             `json:"remaining-receivers"`
	DataPath           string          `json:"data-path"`
	ConsumedTimestamp  time.Time       `json:"consumed-timestamp"`
}

var (
	objectsBucket       []byte
	webhooksBucket      []byte
	notificationsBucket []byte
	timebaseBucket      []byte
)

// Init initializes the InMemory store
func (store *BoltStorage) Init() common.SyncServiceError {
	store.lockChannel = make(chan int, 1)
	store.lockChannel <- 1

	path := common.Configuration.PersistenceRootPath + "/sync/db"
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return err
	}
	store.db, err = bolt.Open(path+"/sync.db", 0600, nil)
	if err != nil {
		return err
	}

	objectsBucket = []byte(objects)
	webhooksBucket = []byte(webhooks)
	notificationsBucket = []byte(notifications)
	timebaseBucket = []byte(timebaseBucketName)

	err = store.db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(objectsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(webhooksBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(notificationsBucket)
		if err != nil {
			return err
		}
		b, err := tx.CreateBucketIfNotExists(timebaseBucket)
		if err != nil {
			return err
		}
		currentTime := time.Now().UnixNano()
		store.timebase = currentTime
		currentTimeInSeconds := currentTime / 1e9
		persistedTimeBase := currentTimeInSeconds
		encoded := b.Get([]byte("timebase"))
		if encoded != nil {
			if err := json.Unmarshal(encoded, &persistedTimeBase); err == nil &&
				currentTimeInSeconds <= persistedTimeBase {
				persistedTimeBase++
				store.timebase = persistedTimeBase * 1e9
			}
		}
		encoded, err = json.Marshal(persistedTimeBase)
		if err != nil {
			return err
		}
		err = tx.Bucket(timebaseBucket).Put([]byte("timebase"), []byte(encoded))
		return err
	})
	if err != nil {
		return err
	}

	path = common.Configuration.PersistenceRootPath + "/sync/local/"
	err = os.MkdirAll(path, 0755)
	store.localDataPath = "file://" + path
	return err
}

// Stop stops the InMemory store
func (store *BoltStorage) Stop() {
	store.db.Close()
}

// PerformMaintenance performs store's maintenance
func (store *BoltStorage) PerformMaintenance() {}

// Cleanup erase the on disk Bolt database
func (store *BoltStorage) Cleanup() {
	os.Remove(common.Configuration.PersistenceRootPath + "/sync/db/sync.db")
}

// StoreObject stores an object
func (store *BoltStorage) StoreObject(metaData common.MetaData, data []byte, status string) common.SyncServiceError {
	// If the object was receieved from a service (status NotReadyToSend/ReadyToSend), i.e. this node is the origin of the object,
	// set instance id. If the object was received from the other side, this node is the receiver of the object:
	// keep the instance id of the meta data.
	if status == common.NotReadyToSend || status == common.ReadyToSend {
		metaData.InstanceID = store.getInstanceID()
	}

	var function func(object boltObject) (boltObject, common.SyncServiceError)
	if metaData.MetaOnly {
		function = func(object boltObject) (boltObject, common.SyncServiceError) {
			object.Meta = metaData
			object.Status = status
			object.RemainingConsumers = metaData.ExpectedConsumers
			object.RemainingReceivers = metaData.ExpectedConsumers
			return object, nil
		}
		return store.updateObjectHelper(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, function)
	}

	dataPath := ""
	if !metaData.NoData && data != nil {
		dataPath = createDataPath(store.localDataPath, metaData)
		if _, err := dataURI.StoreData(dataPath, bytes.NewReader(data), uint32(len(data))); err != nil {
			return err
		}
	}
	object := boltObject{Meta: metaData, Status: status, RemainingConsumers: metaData.ExpectedConsumers,
		RemainingReceivers: metaData.ExpectedConsumers, DataPath: dataPath}
	encoded, err := json.Marshal(object)
	if err != nil {
		return err
	}

	id := getObjectCollectionID(metaData)
	err = store.db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket(objectsBucket).Put([]byte(id), []byte(encoded))
		return err
	})

	return err
}

// StoreObjectData stores an object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *BoltStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {

	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if object.Status == common.NotReadyToSend {
			object.Status = common.ReadyToSend
		} else if object.Status == common.ReadyToSend {
			object.Meta.InstanceID = store.getInstanceID()
		}

		dataPath := createDataPath(store.localDataPath, object.Meta)
		written, err := dataURI.StoreData(dataPath, dataReader, 0)
		if err != nil {
			return object, err
		}
		object.DataPath = dataPath
		object.Meta.ObjectSize = written

		return object, nil
	}
	if err := store.updateObjectHelper(orgID, objectType, objectID, function); err != nil {
		if err == notFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *BoltStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	var meta *common.MetaData
	function := func(object boltObject) common.SyncServiceError {
		meta = &object.Meta
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		return nil, err
	}
	return meta, nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *BoltStorage) RetrieveObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	var dataReader io.Reader
	function := func(object boltObject) common.SyncServiceError {
		var err error
		if object.DataPath != "" {
			dataReader, err = dataURI.GetData(object.DataPath)
			return err
		}
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		if err == notFound {
			return nil, nil
		}
		return nil, err
	}
	return dataReader, nil
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *BoltStorage) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	var meta *common.MetaData
	var status string
	function := func(object boltObject) common.SyncServiceError {
		meta = &object.Meta
		status = object.Status
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		return nil, "", err
	}
	return meta, status, nil
}

// RetrieveObjectStatus finds the object and returns its status
func (store *BoltStorage) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	var status string
	function := func(object boltObject) common.SyncServiceError {
		status = object.Status
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		return "", err
	}
	return status, nil
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed
// If received is true, return objects marked as received
func (store *BoltStorage) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	result := make([]common.MetaData, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && objectType == object.Meta.ObjectType &&
			(object.Status == common.CompletelyReceived || object.Status == common.ObjDeleted ||
				(object.Status == common.ObjReceived && received)) {
			result = append(result, object.Meta)
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination
func (store *BoltStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	result := make([]common.MetaData, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && !object.Meta.Inactive &&
			object.Status == common.ReadyToSend &&
			(object.Meta.DestType == "" || object.Meta.DestType == destType) &&
			(object.Meta.DestID == "" || object.Meta.DestID == destID) {
			result = append(result, object.Meta)
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
func (store *BoltStorage) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	result := make([]common.ConsumedObject, 0)
	function := func(object boltObject) {
		if object.Status == common.ConsumedByDest {
			result = append(result, common.ConsumedObject{MetaData: object.Meta, Timestamp: object.ConsumedTimestamp})
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *BoltStorage) GetObjectsToActivate() ([]common.MetaData, []string, common.SyncServiceError) {
	currentTime := time.Now().Format(time.RFC3339)
	result := make([]common.MetaData, 0)
	statuses := make([]string, 0)
	function := func(object boltObject) {
		if (object.Status == common.NotReadyToSend || object.Status == common.ReadyToSend) &&
			object.Meta.Inactive && object.Meta.ActivationTime <= currentTime {
			result = append(result, object.Meta)
			statuses = append(statuses, object.Status)
		}
	}

	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, nil, err
	}

	return result, statuses, nil
}

// AppendObjectData appends a chunk of data to the object's data
func (store *BoltStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader, dataLength uint32,
	offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError {

	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		dataPath := object.DataPath
		if dataPath == "" {
			if !isFirstChunk {
				return object, &Error{"No path to store data"}
			}
			dataPath = createDataPath(store.localDataPath, object.Meta)
			object.DataPath = dataPath
		}
		return object, dataURI.AppendData(dataPath, dataReader, dataLength, offset, total, isFirstChunk, isLastChunk)
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// UpdateObjectStatus updates an object's status
func (store *BoltStorage) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.Status = status
		if status == common.ConsumedByDest {
			object.ConsumedTimestamp = time.Now()
		}
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// UpdateObjectSourceDataURI pdates object's source data URI
func (store *BoltStorage) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.Meta.SourceDataURI = sourceDataURI
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// RetrieveObjectRemainingConsumers finds the object and returns the number of remaining consumers
// that haven't consumed the object yet
func (store *BoltStorage) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {
	var remainingConsumers int
	function := func(object boltObject) common.SyncServiceError {
		remainingConsumers = object.RemainingConsumers
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		return 0, err
	}
	return remainingConsumers, nil
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *BoltStorage) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.RemainingConsumers = object.Meta.ExpectedConsumers
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *BoltStorage) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	var remainingConsumers int
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.RemainingConsumers--
		remainingConsumers = object.RemainingConsumers
		return object, nil
	}
	if err := store.updateObjectHelper(orgID, objectType, objectID, function); err != nil {
		return 0, err
	}

	return remainingConsumers, nil
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *BoltStorage) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	var remainingReceivers int
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.RemainingReceivers--
		remainingReceivers = object.RemainingReceivers
		return object, nil
	}
	if err := store.updateObjectHelper(orgID, objectType, objectID, function); err != nil {
		return 0, err
	}

	return remainingReceivers, nil
}

// CloseDataReader closes the data reader if necessary
func (store *BoltStorage) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	switch v := dataReader.(type) {
	case *os.File:
		return v.Close()
	}
	return nil
}

// ReadObjectData returns the object data with the specified parameters
func (store *BoltStorage) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) (data []byte,
	eof bool, length int, err common.SyncServiceError) {
	function := func(object boltObject) common.SyncServiceError {
		if object.DataPath != "" {
			data, eof, length, err = dataURI.GetDataChunk(object.DataPath, size, offset)
			return err
		}
		eof = true
		return nil
	}
	err = store.viewObjectHelper(orgID, objectType, objectID, function)
	return
}

// MarkObjectDeleted marks the object as deleted
func (store *BoltStorage) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.Status = common.ObjDeleted
		object.Meta.Deleted = true
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// ActivateObject marks object as active
func (store *BoltStorage) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.Meta.Inactive = false
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// DeleteStoredObject deletes the object
func (store *BoltStorage) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	if err := store.DeleteStoredData(orgID, objectType, objectID); err != nil {
		return nil
	}
	id := createObjectCollectionID(orgID, objectType, objectID)
	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(objectsBucket).Delete([]byte(id))
		return err
	})
	return err
}

// DeleteStoredData deletes the object's data
func (store *BoltStorage) DeleteStoredData(orgID string, objectType string, objectID string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if object.DataPath == "" {
			return object, nil
		}
		if err := dataURI.DeleteStoredData(object.DataPath); err != nil {
			return object, err
		}
		object.DataPath = ""
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *BoltStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	return []common.Destination{common.Destination{DestOrgID: metaData.DestOrgID, DestType: common.Configuration.DestinationType,
		DestID: common.Configuration.DestinationID, Communication: common.Configuration.CommunicationProtocol}}, nil
}

// UpdateObjectDeliveryStatus changes the object's delivery status for the destination
func (store *BoltStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) common.SyncServiceError {
	return nil
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *BoltStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	return nil
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *BoltStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	return nil, nil
}

// AddWebhook stores a webhook for an object type
func (store *BoltStorage) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	function := func(hooks []string) []string {
		// Don't add the webhook if it already is in the list
		for _, hook := range hooks {
			if url == hook {
				return hooks
			}
		}
		if hooks == nil {
			hooks = make([]string, 0)
		}
		hooks = append(hooks, url)
		return hooks
	}
	return store.updateWebhookHelper(objectType, function)
}

// DeleteWebhook deletes a webhook for an object type
func (store *BoltStorage) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	function := func(hooks []string) []string {
		if hooks == nil {
			return nil
		}
		for i, hook := range hooks {
			if strings.EqualFold(hook, url) {
				hooks[i] = hooks[len(hooks)-1]
				return hooks[:len(hooks)-1]
			}
		}
		return nil
	}
	return store.updateWebhookHelper(objectType, function)
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *BoltStorage) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	var encoded []byte
	store.db.View(func(tx *bolt.Tx) error {
		encoded = tx.Bucket(webhooksBucket).Get([]byte(objectType))
		return nil
	})

	if encoded == nil {
		return nil, &NotFound{"No webhooks"}
	}

	var hooks []string
	if err := json.Unmarshal(encoded, &hooks); err != nil {
		return nil, err
	}
	if len(hooks) == 0 {
		return nil, &NotFound{"No webhooks"}
	}
	return hooks, nil
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *BoltStorage) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	return nil, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *BoltStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	return true, nil
}

// StoreDestination stores a destination
func (store *BoltStorage) StoreDestination(destination common.Destination) common.SyncServiceError {
	return nil
}

// DeleteDestination deletes a destination
func (store *BoltStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *BoltStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	return nil
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *BoltStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {}

// RetrieveDestination retrieves a destination
func (store *BoltStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	return &common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID,
		Communication: common.Configuration.CommunicationProtocol}, nil
}

// RetrieveDestinationProtocol retrieves communication protocol for the destination
func (store *BoltStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	return common.Configuration.CommunicationProtocol, nil
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *BoltStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	return nil, nil
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *BoltStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	notification.ResendTime = time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
	function := func(*common.Notification) (*common.Notification, common.SyncServiceError) {
		return &notification, nil
	}
	return store.updateNotificationHelper(notification, function)
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *BoltStorage) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {
	resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
	function := func(notification *common.Notification) (*common.Notification, common.SyncServiceError) {
		if notification != nil {
			notification.ResendTime = resendTime
			return notification, nil
		}
		return nil, notFound
	}
	return store.updateNotificationHelper(notification, function)
}

// RetrieveNotificationRecord retrieves notification
func (store *BoltStorage) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {
	var notification common.Notification
	function := func(n common.Notification) common.SyncServiceError {
		notification = n
		return nil
	}
	if err := store.viewNotificationHelper(orgID, objectType, objectID, destType, destID, function); err != nil {
		return nil, err
	}
	return &notification, nil
}

// DeleteNotificationRecords deletes notification records to an object
func (store *BoltStorage) DeleteNotificationRecords(orgID string, objectType string, objectID string,
	destType string, destID string) common.SyncServiceError {
	var function func(notification common.Notification) bool

	if objectType != "" && objectID != "" {
		if destType != "" && destID != "" {
			id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
			err := store.db.Update(func(tx *bolt.Tx) error {
				err := tx.Bucket(notificationsBucket).Delete([]byte(id))
				return err
			})
			return err
		}
		function = func(notification common.Notification) bool {
			if notification.ObjectType == objectType && notification.ObjectID == objectID {
				return true
			}
			return false
		}
	} else {
		function = func(notification common.Notification) bool {
			if (notification.DestType == destType || destType == "") && (notification.DestID == destID || destID == "") {
				return true
			}
			return false
		}
	}
	return store.deleteNotificationsHelper(function)
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *BoltStorage) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError) {
	var function func(notification common.Notification)
	result := make([]common.Notification, 0)

	if destID != "" && destType != "" {
		function = func(notification common.Notification) {
			if notification.DestType == destType && notification.DestID == destID && resendNotification(notification) {
				result = append(result, notification)
			}
		}
	} else {
		currentTime := time.Now().Unix()
		function = func(notification common.Notification) {
			if resendNotification(notification) {
				if notification.ResendTime <= currentTime || notification.Status == common.Getdata {
					result = append(result, notification)
				}
			}
		}
	}
	if err := store.retrieveNotificationsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *BoltStorage) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	return nil, nil
}

// InsertInitialLeader inserts the initial leader entry
func (store *BoltStorage) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {
	return true, nil
}

// LeaderPeriodicUpdate does the periodic update of the leader entry by the leader
func (store *BoltStorage) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {
	return false, nil
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *BoltStorage) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	return "", 0, time.Now(), 0, nil
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *BoltStorage) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {
	return false, nil
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *BoltStorage) ResignLeadership(leaderID string) common.SyncServiceError {
	return nil
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *BoltStorage) RetrieveTimeOnServer() (time.Time, error) {
	return time.Now(), nil
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *BoltStorage) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {
	return nil
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *BoltStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	return nil
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *BoltStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	return "", nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *BoltStorage) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup, common.SyncServiceError) {
	return nil, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *BoltStorage) DeleteOrganization(orgID string) common.SyncServiceError {
	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *BoltStorage) IsConnected() bool {
	return true
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *BoltStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {
	return time.Now(), nil
}

// RetrieveOrganizationInfo retrieves organization information
func (store *BoltStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *BoltStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	return nil
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *BoltStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *BoltStorage) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	return nil, nil
}

// AddUsersToACL adds users to an ACL
func (store *BoltStorage) AddUsersToACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return nil
}

// RemoveUsersFromACL removes users from an ACL
func (store *BoltStorage) RemoveUsersFromACL(aclType string, orgID string, key string, usernames []string) common.SyncServiceError {
	return nil
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *BoltStorage) RetrieveACL(aclType string, orgID string, key string) ([]string, common.SyncServiceError) {
	return nil, nil
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *BoltStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return nil, nil
}

func (store *BoltStorage) getInstanceID() int64 {
	store.lock()
	defer store.unLock()
	store.timebase++
	return store.timebase
}
