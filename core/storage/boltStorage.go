package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
	bolt "go.etcd.io/bbolt"
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
	Meta                             common.MetaData                 `json:"meta"`
	Status                           string                          `json:"status"`
	PolicyReceived                   bool                            `json:"policy-received"`
	RemainingConsumers               int                             `json:"remaining-consumers"`
	RemainingReceivers               int                             `json:"remaining-receivers"`
	DataPath                         string                          `json:"data-path"`
	ConsumedTimestamp                time.Time                       `json:"consumed-timestamp"`
	Destinations                     []common.StoreDestinationStatus `json:"destinations"`
	RemovedDestinationPolicyServices []common.ServiceID              `json:"removed-destination-policy-services"`
}

type boltDestination struct {
	Destination  common.Destination `json:"destination"`
	LastPingTime time.Time          `json:"last-ping-time"`
}

type boltMessagingGroup struct {
	OrgID      string    `json:"orgid"`
	GroupName  string    `json:"group-name"`
	LastUpdate time.Time `json:"last-update"`
}

type boltACL struct {
	Users   []common.ACLentry `json:"users"`
	OrgID   string            `json:"org-id"`
	ACLType string            `json:"acl-type"`
	Key     string            `json:"key"`
}

var (
	objectsBucket         []byte
	webhooksBucket        []byte
	notificationsBucket   []byte
	timebaseBucket        []byte
	destinationsBucket    []byte
	messagingGroupsBucket []byte
	organizationsBucket   []byte
	aclBucket             []byte
)

// Init initializes the Bolt store
func (store *BoltStorage) Init() common.SyncServiceError {
	store.lockChannel = make(chan int, 1)
	store.lockChannel <- 1

	path := common.Configuration.PersistenceRootPath + "/sync/db/"

	err := os.MkdirAll(path, 0750)
	if err != nil {
		return err
	}
	name := "css-sync.db"
	if common.Configuration.NodeType == common.ESS {
		name = "ess-sync.db"
	}

	store.db, err = bolt.Open(path+name, 0600, nil)
	if err != nil {
		return err
	}

	objectsBucket = []byte(objects)
	webhooksBucket = []byte(webhooks)
	notificationsBucket = []byte(notifications)
	timebaseBucket = []byte(timebaseBucketName)
	destinationsBucket = []byte(destinations)
	messagingGroupsBucket = []byte(messagingGroups)
	organizationsBucket = []byte(organizations)
	aclBucket = []byte(acls)

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
		_, err = tx.CreateBucketIfNotExists(destinationsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(messagingGroupsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(organizationsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(aclBucket)
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

	if len(common.Configuration.ObjectsDataPath) > 0 {
		path = common.Configuration.ObjectsDataPath
	} else {
		path = common.Configuration.PersistenceRootPath + "/sync/local/"
	}
	err = os.MkdirAll(path, 0750)
	store.localDataPath = "file://" + path
	if err == nil {
		common.HealthStatus.ReconnectedToDatabase()
	}
	return err
}

// Stop stops the Bolt store
func (store *BoltStorage) Stop() {
	store.db.Close()
}

// PerformMaintenance performs store's maintenance
func (store *BoltStorage) PerformMaintenance() {
	if common.Configuration.NodeType == common.CSS {
		currentTime := time.Now().UTC().Format(time.RFC3339)

		function := func(object boltObject) bool {
			if object.Meta.Expiration != "" && object.Meta.Expiration <= currentTime &&
				(object.Status == common.ReadyToSend || object.Status == common.NotReadyToSend) {
				return true
			}
			return false
		}

		err := store.deleteObjectsAndNotificationsHelper(function)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in PerformMaintenance: failed to remove expired objects. Error: %s\n", err)
			}
		} else if trace.IsLogging(logger.TRACE) {
			trace.Trace("Removing expired objects")
		}
	}
}

// Cleanup erase the on disk Bolt database only for ESS and test
func (store *BoltStorage) Cleanup(isTest bool) common.SyncServiceError {
	var essDbPath string
	if isTest {
		cssDbPath := common.Configuration.PersistenceRootPath + "/sync/db/css-sync.db"
		essDbPath = common.Configuration.PersistenceRootPath + "/sync/db/ess-sync.db"

		if err := os.Remove(cssDbPath); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to remove css Bolt testing database")
			}
			return err
		}
		if log.IsLogging(logger.DEBUG) {
			log.Debug("CSS bolt db at %s is removed\n", cssDbPath)
		}

	} else {
		essDbPath = common.Configuration.PersistenceRootPath + "/sync/db/ess-sync.db"

	}

	if err := os.Remove(essDbPath); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to remove ess Bolt database")
		}
		return err
	}
	if log.IsLogging(logger.DEBUG) {
		log.Debug("ESS bolt db at %s is removed\n", essDbPath)
	}
	return nil
}

// StoreObject stores an object
// If the object already exists, return the changes in its destinations list (for CSS) - return the list of deleted destinations
func (store *BoltStorage) StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	var dests []common.StoreDestinationStatus
	var deletedDests []common.StoreDestinationStatus

	// If the object was receieved from a service (status NotReadyToSend/ReadyToSend), i.e. this node is the origin of the object,
	// set instance id. If the object was received from the other side, this node is the receiver of the object:
	// keep the instance id of the meta data.
	if status == common.NotReadyToSend || status == common.ReadyToSend {
		newID := store.getInstanceID()
		metaData.InstanceID = newID
		if data != nil && !metaData.NoData && !metaData.MetaOnly {
			metaData.DataID = newID
		}

		if common.Configuration.NodeType == common.CSS {
			var err error
			dests, deletedDests, err = createDestinationsFromMeta(store, metaData)
			if err != nil {
				return nil, err
			}
		}
	}

	if metaData.DestinationPolicy != nil {
		metaData.DestinationPolicy.Timestamp = time.Now().UTC().UnixNano()
	}

	if metaData.MetaOnly {
		function := func(object boltObject) (boltObject, common.SyncServiceError) {
			if object.Status == common.ConsumedByDest {
				// On ESS we remove the data of consumed objects, therefore we can't accept "meta only" updates
				return object, &common.InvalidRequest{"Can't update only the meta data of consumed object"}
			}
			if (object.Meta.DestinationPolicy == nil && metaData.DestinationPolicy != nil) ||
				(object.Meta.DestinationPolicy != nil && metaData.DestinationPolicy == nil) {
				return object, &common.InvalidRequest{"Can't update the existence of Destination Policy"}
			}

			metaData.DataID = object.Meta.DataID       // Keep the previous data id
			metaData.PublicKey = object.Meta.PublicKey // Keep the previous publicKey and signature
			metaData.Signature = object.Meta.Signature
			object.Meta = metaData
			object.Status = status
			object.PolicyReceived = false
			object.RemainingConsumers = metaData.ExpectedConsumers
			object.RemainingReceivers = metaData.ExpectedConsumers
			if metaData.DestinationPolicy == nil {
				object.Destinations = dests
			}
			return object, nil
		}
		err := store.updateObjectHelper(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, function)
		if err == nil || err != notFound {
			return deletedDests, err
		}
		// If not found, insert it
	}

	var dataPath string
	if !metaData.NoData && data != nil {
		dataPath = createDataPathFromMeta(store.localDataPath, metaData)
		if _, err := dataURI.StoreData(dataPath, bytes.NewReader(data), uint32(len(data))); err != nil {
			return nil, err
		}
	} else if !metaData.MetaOnly {
		if err := dataURI.DeleteStoredData(createDataPathFromMeta(store.localDataPath, metaData), false); err != nil {
			return nil, err
		}
	}
	newObject := boltObject{Meta: metaData, Status: status, PolicyReceived: false,
		RemainingConsumers: metaData.ExpectedConsumers, RemainingReceivers: metaData.ExpectedConsumers,
		DataPath: dataPath, Destinations: dests}

	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if (object.Meta.DestinationPolicy == nil && metaData.DestinationPolicy != nil) ||
			(object.Meta.DestinationPolicy != nil && metaData.DestinationPolicy == nil) {
			return object, &common.InvalidRequest{Message: "Can't update the existence of Destination Policy"}
		}

		if metaData.DestinationPolicy != nil {
			newObject.Destinations = object.Destinations
		}
		return newObject, nil
	}
	err := store.updateObjectHelper(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, function)
	if err == notFound {
		// Not found, insert
		encoded, err := json.Marshal(newObject)
		if err != nil {
			return nil, err
		}
		id := getObjectCollectionID(metaData)
		err = store.db.Update(func(tx *bolt.Tx) error {
			err = tx.Bucket(objectsBucket).Put([]byte(id), []byte(encoded))
			return err
		})
		return deletedDests, err
	}
	return deletedDests, err
}

// StoreObjectData stores an object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *BoltStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {

	dataPath := createDataPath(store.localDataPath, orgID, objectType, objectID)
	written, err := dataURI.StoreData(dataPath, dataReader, 0)
	if err != nil {
		return false, err
	}

	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if object.Status == common.NotReadyToSend {
			object.Status = common.ReadyToSend
		}
		if object.Status == common.NotReadyToSend || object.Status == common.ReadyToSend {
			newID := store.getInstanceID()
			object.Meta.InstanceID = newID
			object.Meta.DataID = newID
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

func (store *BoltStorage) StoreObjectTempData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	dataPath := createDataPath(store.localDataPath, orgID, objectType, objectID)
	_, err := dataURI.StoreTempData(dataPath, dataReader, 0)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (store *BoltStorage) RemoveObjectTempData(orgID string, objectType string, objectID string) common.SyncServiceError {
	dataPath := createDataPath(store.localDataPath, orgID, objectType, objectID)
	if err := dataURI.DeleteStoredData(dataPath, true); err != nil {
		if common.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func (store *BoltStorage) RetrieveObjectTempData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	var dataReader io.Reader
	dataPath := createDataPath(store.localDataPath, orgID, objectType, objectID)
	dataReader, err := dataURI.GetData(dataPath, true)
	if err != nil {
		if common.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return dataReader, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *BoltStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	var meta *common.MetaData
	function := func(object boltObject) common.SyncServiceError {
		meta = &object.Meta
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		if common.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return meta, nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *BoltStorage) RetrieveObjectData(orgID string, objectType string, objectID string, isTempData bool) (io.Reader, common.SyncServiceError) {
	var dataReader io.Reader
	function := func(object boltObject) common.SyncServiceError {
		var err error
		if object.DataPath != "" {
			dataReader, err = dataURI.GetData(object.DataPath, isTempData)
			return err
		}
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		if common.IsNotFound(err) {
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
		if common.IsNotFound(err) {
			err = nil
		}
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
		if common.IsNotFound(err) {
			err = nil
		}
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
	if len(common.Configuration.ObjectsDataPath) > 0 {
		for i := 0; i < len(result); i++ {
			result[i].DestinationDataURI = createDataPathFromMeta(store.localDataPath, result[i])
		}
	}
	return result, nil
}

// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
// If received is true, return objects marked as policy received
func (store *BoltStorage) RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	result := make([]common.ObjectDestinationPolicy, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && object.Meta.DestinationPolicy != nil &&
			(received || !object.PolicyReceived) {
			result = append(result, createObjectDestinationPolicy(object))
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
func (store *BoltStorage) RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	result := make([]common.ObjectDestinationPolicy, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && object.Meta.DestinationPolicy != nil {
			for _, service := range object.Meta.DestinationPolicy.Services {
				if serviceOrgID == service.OrgID && serviceName == service.ServiceName {
					result = append(result, createObjectDestinationPolicy(object))
				}
			}
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
func (store *BoltStorage) RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	result := make([]common.ObjectDestinationPolicy, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && object.Meta.DestinationPolicy != nil &&
			object.Meta.DestinationPolicy.Timestamp >= since {
			result = append(result, createObjectDestinationPolicy(object))
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjectsWithFilters returns the list of all othe objects that meet the given conditions
func (store *BoltStorage) RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string, deleted *bool) ([]common.MetaData, common.SyncServiceError) {
	result := make([]common.MetaData, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID {
			// check destinationPolicy
			if destinationPolicy != nil {
				if *destinationPolicy {
					if object.Meta.DestinationPolicy != nil && object.Meta.DestinationPolicy.Timestamp >= since {
						check := false
						if dpServiceOrgID != "" && dpServiceName != "" {
							for _, service := range object.Meta.DestinationPolicy.Services {
								if dpServiceOrgID == service.OrgID && dpServiceName == service.ServiceName {
									check = true
									break
								}
							}
							if !check {
								return
							}
						}

						if dpPropertyName != "" {
							check = false
							for _, policyProperty := range object.Meta.DestinationPolicy.Properties {
								if dpPropertyName == policyProperty.Name {
									check = true
									break
								}
							}
							if !check {
								return
							}
						}

						// if check {
						// 	result = append(result, obj.Meta)
						// }
					} else {
						return
					}
				} else { //*destinationPolicy = false
					if object.Meta.DestinationPolicy != nil {
						return
					}

				}
			}

			// check objectType and objectID
			if objectType != "" {
				if objectType != object.Meta.ObjectType {
					return
				}
				if objectID != "" {
					if objectID != object.Meta.ObjectID {
						return
					}
				}
			}

			// check destinationType and destinationID
			if destinationType != "" {
				if object.Meta.DestType != "" {
					if destinationType != object.Meta.DestType {
						return
					}
					if destinationID != "" {
						if destinationID != object.Meta.DestID {
							return
						}
					}

				} else { // check object.Meta.DestinationsList (destinationType: destinationID)
					checkedDestList := false
					for _, dest := range object.Meta.DestinationsList {
						if destinationID != "" {
							if dest == destinationType+":"+destinationID {
								checkedDestList = true
								break
							}
						} else {
							parts := strings.SplitN(dest, ":", 2)
							if len(parts) == 2 && parts[0] == destinationType {
								checkedDestList = true
								break
							}

						}
					}
					if !checkedDestList {
						return
					}
				}

			}

			if noData != nil {
				if *noData != object.Meta.NoData {
					return
				}
			}

			if expirationTimeBefore != "" {
				boltObjectExpirationTime, err := time.Parse(time.RFC3339, object.Meta.Expiration)
				queryExpirationTimeBefore, _ := time.Parse(time.RFC3339, expirationTimeBefore)

				if err != nil || boltObjectExpirationTime.After(queryExpirationTimeBefore) {
					return
				}
			}

			if deleted != nil && *deleted != object.Meta.Deleted {
				return
			}

			// if expirationTimeBefore is not satisfy, return
			result = append(result, object.Meta)

		}

	}

	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil

}

// RetrieveAllObjects returns the list of all the objects of the specified type
func (store *BoltStorage) RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	result := make([]common.ObjectDestinationPolicy, 0)
	function := func(object boltObject) {
		if orgID == object.Meta.DestOrgID && object.Meta.ObjectType == objectType {
			result = append(result, createObjectDestinationPolicy(object))
		}
	}
	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination
// For CSS: adds the new destination to the destinations lists of the relevant objects.
func (store *BoltStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	result := make([]common.MetaData, 0)

	if common.Configuration.NodeType == common.ESS {
		function := func(object boltObject) {
			if (orgID == object.Meta.DestOrgID || orgID == "") && !object.Meta.Inactive &&
				object.Status == common.ReadyToSend &&
				(object.Meta.DestType == "" || object.Meta.DestType == destType || destType == "") &&
				(object.Meta.DestID == "" || object.Meta.DestID == destID || destID == "") {
				result = append(result, object.Meta)
			}
		}
		if err := store.retrieveObjectsHelper(function); err != nil {
			return nil, err
		}
		return result, nil
	}

	function := func(object boltObject) (*boltObject, common.SyncServiceError) {
		if object.Meta.DestinationPolicy == nil && orgID == object.Meta.DestOrgID &&
			(object.Meta.DestType == "" || object.Meta.DestType == destType) &&
			(object.Meta.DestID == "" || object.Meta.DestID == destID) {
			status := common.Pending
			if object.Status == common.ReadyToSend && !object.Meta.Inactive {
				status = common.Delivering
			}
			needToUpdate := false

			// Add destination if it doesn't exist in the destinations list
			if dest, err := store.RetrieveDestination(orgID, destType, destID); err == nil && dest != nil {
				existingDestIndex := -1
				for i, d := range object.Destinations {
					if d.Destination == *dest {
						existingDestIndex = i
						break
					}
				}
				if existingDestIndex != -1 {
					d := object.Destinations[existingDestIndex]
					if status == common.Delivering &&
						(resend == common.ResendAll || (resend == common.ResendDelivered && d.Status != common.Consumed) ||
							(resend == common.ResendUndelivered && d.Status != common.Consumed && d.Status != common.Delivered)) {
						result = append(result, object.Meta)
						object.Destinations[existingDestIndex].Status = common.Delivering
						needToUpdate = true
					}
				} else {
					if status == common.Delivering {
						result = append(result, object.Meta)
					}
					needToUpdate = true
					object.Destinations = append(object.Destinations, common.StoreDestinationStatus{Destination: *dest, Status: status})
				}
				if needToUpdate {
					return &object, nil
				}
				return nil, nil
			}
		}
		return nil, nil
	}
	if err := store.updateObjectsHelper(function); err != nil {
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
func (store *BoltStorage) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	currentTime := time.Now().UTC().Format(time.RFC3339)
	result := make([]common.MetaData, 0)
	function := func(object boltObject) {
		if (object.Status == common.NotReadyToSend || object.Status == common.ReadyToSend) &&
			object.Meta.Inactive && object.Meta.ActivationTime != "" && object.Meta.ActivationTime <= currentTime {
			result = append(result, object.Meta)
		}
	}

	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}

	return result, nil
}

// AppendObjectData appends a chunk of data to the object's data
func (store *BoltStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader, dataLength uint32,
	offset int64, total int64, isFirstChunk bool, isLastChunk bool, isTempData bool) (bool, common.SyncServiceError) {

	dataPath := ""
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		dataPath = object.DataPath
		if dataPath == "" {
			if !isFirstChunk {
				return object, &Error{"No path to store data"}
			}
			dataPath = createDataPathFromMeta(store.localDataPath, object.Meta)
			object.DataPath = dataPath
		}
		return object, nil
	}
	if err := store.updateObjectHelper(orgID, objectType, objectID, function); err != nil {
		return isLastChunk, err
	}
	return dataURI.AppendData(dataPath, dataReader, dataLength, offset, total, isFirstChunk, isLastChunk, isTempData)
}

// Handles the last data chunk
func (store *BoltStorage) HandleObjectInfoForLastDataChunk(orgID string, objectType string, objectID string, isTempData bool, dataSize int64) (bool, common.SyncServiceError) {
	//dataPath := createDataPath(store.localDataPath, orgID, objectType, objectID)
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if object.Status == common.NotReadyToSend {
			object.Status = common.ReadyToSend
		}
		if object.Status == common.NotReadyToSend || object.Status == common.ReadyToSend {
			newID := store.getInstanceID()
			object.Meta.InstanceID = newID
			object.Meta.DataID = newID
		}

		//object.DataPath = dataPath
		object.Meta.ObjectSize = dataSize

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

// UpdateObjectDataVerifiedStatus updates object's dataVerified field
func (store *BoltStorage) UpdateObjectDataVerifiedStatus(orgID string, objectType string, objectID string, verified bool) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.Meta.DataVerified = verified
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

// MarkDestinationPolicyReceived marks an object's destination policy as having been received
func (store *BoltStorage) MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.PolicyReceived = true
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
	if err := store.DeleteStoredData(orgID, objectType, objectID, false); err != nil {
		return nil
	}
	if err := store.DeleteStoredData(orgID, objectType, objectID, true); err != nil {
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
func (store *BoltStorage) DeleteStoredData(orgID string, objectType string, objectID string, isTempData bool) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		if object.DataPath == "" {
			return object, nil
		}
		if err := dataURI.DeleteStoredData(object.DataPath, isTempData); err != nil {
			return object, err
		}
		if !isTempData {
			object.DataPath = ""
		}
		return object, nil
	}

	err := store.updateObjectHelper(orgID, objectType, objectID, function)
	if err != nil && err != notFound {
		return err
	}
	return nil
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *BoltStorage) CleanObjects() common.SyncServiceError {
	function := func(object boltObject) bool {
		if object.Status == common.PartiallyReceived {
			return true
		}
		return false
	}

	err := store.deleteObjectsHelper(function)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in CleanObjects: failed to remove objects. Error: %s\n", err)
		}
	}
	return nil
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *BoltStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return []common.Destination{common.Destination{DestOrgID: metaData.DestOrgID, DestType: common.Configuration.DestinationType,
			DestID: common.Configuration.DestinationID, Communication: common.Configuration.CommunicationProtocol}}, nil
	}

	var dests []common.StoreDestinationStatus
	function := func(object boltObject) common.SyncServiceError {
		dests = object.Destinations
		return nil
	}
	if err := store.viewObjectHelper(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, function); err != nil {
		if common.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	destinations := make([]common.Destination, 0)
	for _, d := range dests {
		destinations = append(destinations, d.Destination)
	}
	return destinations, nil
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *BoltStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	var dests []common.StoreDestinationStatus
	function := func(object boltObject) common.SyncServiceError {
		dests = object.Destinations
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		if common.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return dests, nil
}

// UpdateObjectDestinations updates object's destinations
// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
func (store *BoltStorage) UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, "", nil, nil, nil
	}
	var deletedDests, addedDests []common.StoreDestinationStatus
	var metaData common.MetaData
	var status string
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		var dests []common.StoreDestinationStatus
		var err error
		dests, deletedDests, addedDests, err = createDestinations(orgID, store, object.Destinations, destinationsList)
		if err != nil {
			return object, err
		}

		object.Destinations = dests
		metaData = object.Meta
		status = object.Status
		return object, nil
	}

	err := store.updateObjectHelper(orgID, objectType, objectID, function)
	if err != nil {
		return nil, "", nil, nil, err
	}
	return &metaData, status, deletedDests, addedDests, nil
}

// AddObjectdestinations adds the destinations to object's destination list
// Returns the metadata, object's status, an array of added destinations after removing the overlapped destinations
func (store *BoltStorage) AddObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string, []common.StoreDestinationStatus, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, "", nil, nil
	}

	var addedDests []common.StoreDestinationStatus
	var metaData common.MetaData
	var status string
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		var updatedDests []common.StoreDestinationStatus
		var err error

		updatedDests, addedDests, err = getDestinationsForAdd(orgID, store, object.Destinations, destinationsList)
		if err != nil {
			return object, err
		}

		object.Destinations = updatedDests
		metaData = object.Meta
		status = object.Status
		return object, nil
	}

	err := store.updateObjectHelper(orgID, objectType, objectID, function)
	if err != nil {
		return nil, "", nil, err
	}

	return &metaData, status, addedDests, nil
}

// DeleteObjectdestinations deletes the destinations from object's destination list
// Returns the metadata, objects' status, an array of destinations that removed from the current destination list
func (store *BoltStorage) DeleteObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string, []common.StoreDestinationStatus, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, "", nil, nil
	}

	var deletedDests []common.StoreDestinationStatus
	var metaData common.MetaData
	var status string
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		var updatedDests []common.StoreDestinationStatus
		var err error

		updatedDests, deletedDests, err = getDestinationsForDelete(orgID, store, object.Destinations, destinationsList)
		if err != nil {
			return object, err
		}

		object.Destinations = updatedDests
		metaData = object.Meta
		status = object.Status
		return object, nil
	}

	err := store.updateObjectHelper(orgID, objectType, objectID, function)
	if err != nil {
		return nil, "", nil, err
	}

	return &metaData, status, deletedDests, nil
}

// UpdateObjectDeliveryStatus changes the object's delivery status for the destination
// Returns true if the status is Deleted and all the destinations are in status Deleted
func (store *BoltStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) (bool, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return true, nil
	}

	allDeleted := true
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		found := false
		allConsumed := true
		for i, d := range object.Destinations {
			if !found && d.Destination.DestType == destType && d.Destination.DestID == destID {
				if message != "" || d.Status == common.Error {
					object.Destinations[i].Message = message
				}
				if status != "" {
					object.Destinations[i].Status = status
				}
				found = true
			} else {
				if d.Status != common.Consumed {
					allConsumed = false
				}
				if d.Status != common.Deleted {
					allDeleted = false
				}
			}
		}
		if !found {
			return object, &Error{"Failed to find destination."}
		}
		if object.Meta.AutoDelete && status == common.Consumed && allConsumed && object.Meta.Expiration == "" {
			// Delete the object by setting its expiration time to one hour
			object.Meta.Expiration = time.Now().Add(time.Hour * time.Duration(1)).UTC().Format(time.RFC3339)
		}
		return object, nil
	}
	err := store.updateObjectHelper(orgID, objectType, objectID, function)
	return (allDeleted && status == common.Deleted), err
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *BoltStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		for i := range object.Destinations {
			object.Destinations[i].Status = common.Delivering
		}
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// GetNumberOfStoredObjects returns the number of objects received from the application that are
// currently stored in this node's storage
func (store *BoltStorage) GetNumberOfStoredObjects() (uint32, common.SyncServiceError) {
	var count uint32
	function := func(object boltObject) {
		if object.Status == common.ReadyToSend || object.Status == common.NotReadyToSend {
			count++
		}
	}

	if err := store.retrieveObjectsHelper(function); err != nil {
		return 0, err
	}
	return count, nil
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
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]common.Destination, 0)
	function := func(dest boltDestination) {
		if (orgID == "" || orgID == dest.Destination.DestOrgID) &&
			(destType == "" || destType == dest.Destination.DestType) {
			result = append(result, dest.Destination)
		}
	}

	if err := store.retrieveDestinationsHelper(function); err != nil {
		return nil, err
	}

	return result, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *BoltStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return true, nil
	}

	exists := false
	function := func(dest boltDestination) {
		if orgID == dest.Destination.DestOrgID && destType == dest.Destination.DestType && destID == dest.Destination.DestID {
			exists = true
		}
	}

	if err := store.retrieveDestinationsHelper(function); err != nil {
		return false, err
	}
	return exists, nil
}

// StoreDestination stores a destination
func (store *BoltStorage) StoreDestination(destination common.Destination) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	dest := boltDestination{Destination: destination, LastPingTime: time.Now()}
	encoded, err := json.Marshal(dest)
	if err != nil {
		return err
	}

	id := getDestinationCollectionID(destination)
	err = store.db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket(destinationsBucket).Put([]byte(id), []byte(encoded))
		return err
	})
	return err
}

// DeleteDestination deletes a destination
func (store *BoltStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	id := createDestinationCollectionID(orgID, destType, destID)
	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(destinationsBucket).Delete([]byte(id))
		return err
	})
	return err
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *BoltStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	function := func(dest boltDestination) boltDestination {
		dest.LastPingTime = time.Now()
		return dest
	}
	id := getDestinationCollectionID(destination)
	return store.updateDestinationHelper(id, function)
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *BoltStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {
	if common.Configuration.NodeType == common.ESS {
		return
	}

	toBeDeleted := make([]common.Destination, 0)
	function := func(dest boltDestination) bool {
		if dest.LastPingTime.Before(lastTimestamp) {
			toBeDeleted = append(toBeDeleted, dest.Destination)
			return true
		}
		return false
	}

	err := store.deleteDestinationsHelper(function)
	if err != nil && log.IsLogging(logger.ERROR) {
		log.Error("Error in boltStorage.RemoveInactiveDestinations: failed to remove inactive destination. Error: %s\n", err)
	}

	for _, dest := range toBeDeleted {
		if err := store.DeleteNotificationRecords(dest.DestOrgID, "", "", dest.DestType, dest.DestID); err != nil && log.IsLogging(logger.ERROR) {
			log.Error("Error in boltStorage.RemoveInactiveDestinations: failed to remove notifications. Error: %s\n", err)
		}
	}
}

// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
func (store *BoltStorage) GetNumberOfDestinations() (uint32, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return 0, nil
	}
	var count uint32
	function := func(dest boltDestination) {
		count++
	}

	if err := store.retrieveDestinationsHelper(function); err != nil {
		return 0, err
	}
	return count, nil
}

// RetrieveDestination retrieves a destination
func (store *BoltStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return &common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID,
			Communication: common.Configuration.CommunicationProtocol}, nil
	}

	var dest *common.Destination
	function := func(d boltDestination) common.SyncServiceError {
		dest = &d.Destination
		return nil
	}
	if err := store.viewDestinationHelper(orgID, destType, destID, function); err != nil && err != notFound {
		return nil, err
	}
	return dest, nil
}

// RetrieveDestinationProtocol retrieves communication protocol for the destination
func (store *BoltStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return common.Configuration.CommunicationProtocol, nil
	}

	var protocol string
	function := func(d boltDestination) common.SyncServiceError {
		protocol = d.Destination.Communication
		return nil
	}
	if err := store.viewDestinationHelper(orgID, destType, destID, function); err != nil && err != notFound {
		return "", err
	}
	return protocol, nil
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *BoltStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}
	notificationRecords := make([]common.Notification, 0)
	function := func(notification common.Notification) {
		if notification.DestOrgID == orgID && notification.DestType == destType && notification.DestID == destID &&
			(notification.Status == common.Update || notification.Status == common.UpdatePending || notification.Status == common.Updated ||
				notification.Status == common.ReceivedByDestination || notification.Status == common.ConsumedByDestination ||
				notification.Status == common.Error) {
			notificationRecords = append(notificationRecords, notification)
		}
	}

	if err := store.retrieveNotificationsHelper(function); err != nil {
		return nil, err
	}

	var status string
	objectStatuses := make([]common.ObjectStatus, 0)
	for _, n := range notificationRecords {
		switch n.Status {
		case common.Update:
			status = common.Delivering
		case common.UpdatePending:
			status = common.Delivering
		case common.Updated:
			status = common.Delivering
		case common.ReceivedByDestination:
			status = common.Delivered
		case common.ConsumedByDestination:
			status = common.Consumed
		case common.Error:
			status = common.Error
		}
		objectStatus := common.ObjectStatus{OrgID: orgID, ObjectType: n.ObjectType, ObjectID: n.ObjectID, Status: status}
		objectStatuses = append(objectStatuses, objectStatus)
	}
	return objectStatuses, nil
}

// RetrieveAllObjectsAndUpdateDestinationListForDestination retrieves objects that are in use on a given node and returns the list of metadata
func (store *BoltStorage) RetrieveAllObjectsAndUpdateDestinationListForDestination(destOrgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError) {
	// 1. retrieve metadata
	metadataList := make([]common.MetaData, 0)

	function := func(object boltObject) {
		for _, destination := range object.Destinations {
			if destOrgID == destination.Destination.DestOrgID && destType == destination.Destination.DestType && destID == destination.Destination.DestID {
				metadataList = append(metadataList, object.Meta)
				break
			}
		}
	}

	if err := store.retrieveObjectsHelper(function); err != nil {
		return nil, err
	}

	// 2. update destination
	function2 := func(object boltObject) (*boltObject, common.SyncServiceError) {
		found := false
		updatedDestinationList := make([]common.StoreDestinationStatus, 0)
		for _, destination := range object.Destinations {
			if destOrgID == destination.Destination.DestOrgID && destType == destination.Destination.DestType && destID == destination.Destination.DestID {
				// find the object to update destinations
				found = true
			} else {
				updatedDestinationList = append(updatedDestinationList, destination)
			}
		}

		if found {
			object.Destinations = updatedDestinationList
			return &object, nil
		}

		return nil, nil
	}

	if err := store.updateObjectsHelper(function2); err != nil {
		return nil, err
	}

	return metadataList, nil
}

// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
func (store *BoltStorage) RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError) {
	var meta *common.MetaData
	var removedDestinationPolicyServices []common.ServiceID
	function := func(object boltObject) common.SyncServiceError {
		meta = &object.Meta
		removedDestinationPolicyServices = object.RemovedDestinationPolicyServices
		return nil
	}
	if err := store.viewObjectHelper(orgID, objectType, objectID, function); err != nil {
		if common.IsNotFound(err) {
			err = nil
		}
		emptyList := make([]common.ServiceID, 0)
		return nil, emptyList, err
	}
	return meta, removedDestinationPolicyServices, nil
}

// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
func (store *BoltStorage) UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError {
	function := func(object boltObject) (boltObject, common.SyncServiceError) {
		object.RemovedDestinationPolicyServices = destinationPolicyServices
		return object, nil
	}
	return store.updateObjectHelper(orgID, objectType, objectID, function)
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *BoltStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	if notification.ResendTime == 0 {
		notification.ResendTime = time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
	}
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
		if err == notFound {
			return nil, nil
		}
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
			if notification.DestOrgID == orgID && notification.ObjectType == objectType && notification.ObjectID == objectID {
				return true
			}
			return false
		}
	} else {
		function = func(notification common.Notification) bool {
			if (notification.DestOrgID == orgID || orgID == "") && (notification.DestType == destType || destType == "") &&
				(notification.DestID == destID || destID == "") {
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
			if notification.DestOrgID == orgID && notification.DestType == destType && notification.DestID == destID &&
				resendNotification(notification, retrieveReceived && common.Configuration.NodeType == common.CSS) {
				result = append(result, notification)
			}
		}
	} else {
		currentTime := time.Now().Unix()
		function = func(notification common.Notification) {
			if resendNotification(notification, false) {
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
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]common.Notification, 0)
	function := func(notification common.Notification) {
		if (orgID == "" || orgID == notification.DestOrgID) && (destType == "" || destType == notification.DestType) &&
			(destID == "" || destID == notification.DestID) &&
			(notification.Status == common.UpdatePending || notification.Status == common.ConsumedPending ||
				notification.Status == common.DeletePending || notification.Status == common.DeletedPending) {
			result = append(result, notification)
		}
	}
	if err := store.retrieveNotificationsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
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
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	mg := boltMessagingGroup{OrgID: orgID, GroupName: messagingGroup, LastUpdate: time.Now()}
	encoded, err := json.Marshal(mg)
	if err != nil {
		return err
	}

	err = store.db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket(messagingGroupsBucket).Put([]byte(orgID), []byte(encoded))
		return err
	})
	return err
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *BoltStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(messagingGroupsBucket).Delete([]byte(orgID))
		return err
	})
	return err
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *BoltStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return "", nil
	}

	var mg boltMessagingGroup
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(messagingGroupsBucket).Get([]byte(orgID))
		if encoded == nil {
			return notFound
		}
		if err := json.Unmarshal(encoded, &mg); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err != notFound {
			return "", err
		}
		return "", nil
	}
	return mg.GroupName, nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *BoltStorage) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]common.MessagingGroup, 0)
	function := func(mg boltMessagingGroup) {
		if mg.LastUpdate.After(time) {
			result = append(result, common.MessagingGroup{OrgID: mg.OrgID, GroupName: mg.GroupName})
		}
	}
	if err := store.retrieveMessagingGroupHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *BoltStorage) DeleteOrganization(orgID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	if err := store.DeleteOrgToMessagingGroup(orgID); err != nil {
		return err
	}

	destFunction := func(dest boltDestination) bool {
		if dest.Destination.DestOrgID == orgID {
			return true
		}
		return false
	}
	if err := store.deleteDestinationsHelper(destFunction); err != nil {
		return &Error{fmt.Sprintf("Failed to delete destinations. Error: %s.", err)}
	}

	notificationFunction := func(notification common.Notification) bool {
		if notification.DestOrgID == orgID {
			return true
		}
		return false
	}
	if err := store.deleteNotificationsHelper(notificationFunction); err != nil {
		return &Error{fmt.Sprintf("Failed to delete notifications. Error: %s.", err)}
	}

	objectFunction := func(object boltObject) bool {
		if object.Meta.DestOrgID == orgID {
			return true
		}
		return false
	}
	if err := store.deleteObjectsHelper(objectFunction); err != nil {
		return &Error{fmt.Sprintf("Failed to delete objects. Error: %s.", err)}
	}

	aclFunction := func(acl boltACL) bool {
		if acl.OrgID == orgID {
			return true
		}
		return false
	}
	if err := store.deleteACLsHelper(aclFunction); err != nil {
		return &Error{fmt.Sprintf("Failed to delete ACLs. Error: %s.", err)}
	}

	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *BoltStorage) IsConnected() bool {
	return true
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *BoltStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {
	currentTime := time.Now()
	if common.Configuration.NodeType == common.ESS {
		return currentTime, nil
	}

	organization := common.StoredOrganization{Org: org, Timestamp: currentTime}
	encoded, err := json.Marshal(organization)
	if err != nil {
		return currentTime, err
	}

	err = store.db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket(organizationsBucket).Put([]byte(org.OrgID), []byte(encoded))
		return err
	})
	return currentTime, err
}

// RetrieveOrganizationInfo retrieves organization information
func (store *BoltStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	var org common.StoredOrganization
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(organizationsBucket).Get([]byte(orgID))
		if encoded == nil {
			return notFound
		}
		if err := json.Unmarshal(encoded, &org); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if err != notFound {
			return nil, err
		}
		return nil, nil
	}
	return &org, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *BoltStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	err := store.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(organizationsBucket).Delete([]byte(orgID))
		return err
	})
	return err
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *BoltStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]common.StoredOrganization, 0)
	function := func(org common.StoredOrganization) {
		result = append(result, org)
	}
	if err := store.retrieveOrganizationsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *BoltStorage) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]common.StoredOrganization, 0)
	function := func(org common.StoredOrganization) {
		if !org.Timestamp.Before(time) {
			result = append(result, org)
		}
	}
	if err := store.retrieveOrganizationsHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// AddUsersToACL adds users to an ACL
func (store *BoltStorage) AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	if key == "" {
		key = "*"
	}

	function := func(acl boltACL) (*boltACL, bool) {
		added := false
		for _, user := range users {
			notFound := true
			// Don't add the user if it already is in the list
			for _, existing := range acl.Users {
				if user.ACLUserType == existing.ACLUserType && user.Username == existing.Username {
					notFound = false
					break
				}
			}
			if notFound {
				acl.Users = append(acl.Users, user)
				added = true
			}
		}
		if added {
			return &acl, false
		}
		return nil, false
	}

	return store.updateACLHelper(aclType, orgID, key, function)
}

// RemoveUsersFromACL removes users from an ACL
func (store *BoltStorage) RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS {
		return nil
	}

	if key == "" {
		key = "*"
	}

	function := func(acl boltACL) (*boltACL, bool) {
		deleted := false
		for _, user := range users {
			for i, entry := range acl.Users {
				if user.ACLUserType == entry.ACLUserType && user.Username == entry.Username {
					if len(acl.Users) == 1 {
						// Deleting the last user, delete the ACL
						return nil, true
					}

					acl.Users[i] = acl.Users[len(acl.Users)-1]
					acl.Users = acl.Users[:len(acl.Users)-1]
					deleted = true
					break
				}
			}
		}
		if deleted {
			return &acl, false
		}
		return nil, false
	}

	return store.updateACLHelper(aclType, orgID, key, function)
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *BoltStorage) RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	var encoded []byte
	if key == "" {
		key = "*"
	}
	store.db.View(func(tx *bolt.Tx) error {
		encoded = tx.Bucket(aclBucket).Get([]byte(orgID + ":" + aclType + ":" + key))
		return nil
	})

	if encoded == nil {
		return make([]common.ACLentry, 0), nil
	}

	var acl boltACL
	if err := json.Unmarshal(encoded, &acl); err != nil {
		return nil, err
	}
	return acl.Users, nil
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *BoltStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}

	result := make([]string, 0)
	function := func(acl boltACL) {
		if acl.ACLType == aclType && acl.OrgID == orgID {
			result = append(result, acl.Key)
		}
	}
	if err := store.retrieveACLHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
func (store *BoltStorage) RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	if common.Configuration.NodeType == common.ESS {
		return nil, nil
	}
	result := make([]string, 0)
	function := func(acl boltACL) {
		if acl.ACLType == aclType && acl.OrgID == orgID {
			for _, user := range acl.Users {
				if aclRole == "" || aclRole == "*" {
					if aclUserType == user.ACLUserType && aclUsername == user.Username {
						result = append(result, acl.Key)
					}
				} else {
					if aclUserType == user.ACLUserType && aclUsername == user.Username && aclRole == user.ACLRole {
						result = append(result, acl.Key)
					}
				}
			}
		}
	}
	if err := store.retrieveACLHelper(function); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *BoltStorage) getInstanceID() int64 {
	store.lock()
	defer store.unLock()
	store.timebase++
	return store.timebase
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *BoltStorage) IsPersistent() bool {
	return true
}
