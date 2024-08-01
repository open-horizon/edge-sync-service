package storage

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type gridfsFile struct {
	Id     string `bson:"_id"`
	Name   string `bson:"filename"`
	Length int64  `bson:"length"`
}

// MongoStorage is a MongoDB based store
type MongoStorage struct {
	client            *mongo.Client
	clientConnectOpts *options.ClientOptions
	database          *mongo.Database
	gridfsBucket      *gridfs.Bucket
	connected         bool
	lockChannel       chan int
	mapLock           chan int
}

type object struct {
	ID                 string                          `bson:"_id"`
	MetaData           common.MetaData                 `bson:"metadata"`
	Status             string                          `bson:"status"`
	PolicyReceived     bool                            `bson:"policy-received"`
	RemainingConsumers int                             `bson:"remaining-consumers"`
	RemainingReceivers int                             `bson:"remaining-receivers"`
	Destinations       []common.StoreDestinationStatus `bson:"destinations"`
	LastUpdate         time.Time                       `bson:"last-update"`
}

type destinationObject struct {
	ID           string             `bson:"_id"`
	Destination  common.Destination `bson:"destination"`
	LastPingTime time.Time          `bson:"last-ping-time"`
}

type notificationObject struct {
	ID           string              `bson:"_id"`
	Notification common.Notification `bson:"notification"`
}

type leaderDocument struct {
	ID               int32     `bson:"_id"`
	UUID             string    `bson:"uuid"`
	LastHeartbeatTS  time.Time `bson:"last-heartbeat-ts"`
	HeartbeatTimeout int32     `bson:"heartbeat-timeout"`
	Version          int64     `bson:"version"`
}

type isMasterResult struct {
	IsMaster  bool      `bson:"isMaster"`
	LocalTime time.Time `bson:"localTime"`
	OK        bool      `bson:"ok"`
}

type messagingGroupObject struct {
	ID         string    `bson:"_id"`
	GroupName  string    `bson:"group-name"`
	LastUpdate time.Time `bson:"last-update"`
}

// This is almost the same type as common.StoredOrganization except for the timestamp type.
// We use this type here to avoid dependency on bson in common.
type organizationObject struct {
	ID           string              `bson:"_id"`
	Organization common.Organization `bson:"org"`
	LastUpdate   time.Time           `bson:"last-update"`
}

type webhookObject struct {
	ID         string    `bson:"_id"`
	Hooks      []string  `bson:"hooks"`
	LastUpdate time.Time `bson:"last-update"`
}

type aclObject struct {
	ID         string            `bson:"_id"`
	Users      []common.ACLentry `bson:"users"`
	OrgID      string            `bson:"org-id"`
	ACLType    string            `bson:"acl-type"`
	LastUpdate time.Time         `bson:"last-update"`
}

type dataInfoObject struct {
	ID         string    `bson:"_id"`
	ChunkSize  int32     `bson:"chunkSize"`
	UploadDate time.Time `bson:"uploadDate"`
	Length     int32     `bson:"length"`
	MD5        string    `bson:"md5"`
	Filename   string    `bson:"filename"`
}

const maxUpdateTries = 5

var sleepInMS int

// Init initializes the MongoStorage store
func (store *MongoStorage) Init() common.SyncServiceError {
	store.lockChannel = make(chan int, 1)
	store.lockChannel <- 1
	store.mapLock = make(chan int, 1)
	store.mapLock <- 1

	/*
		store.dialInfo = &mgo.DialInfo{
			Addrs:        strings.Split(common.Configuration.MongoAddressCsv, ","),
			Source:       common.Configuration.MongoAuthDbName,
			Username:     common.Configuration.MongoUsername,
			Password:     common.Configuration.MongoPassword,
			Timeout:      time.Duration(20 * time.Second),
			ReadTimeout:  time.Duration(60 * time.Second),
			WriteTimeout: time.Duration(60 * time.Second),
		}*/

	var mongoClient *mongo.Client
	var err error
	if trace.IsLogging(logger.INFO) {
		trace.Info("CConnecting to mongo...")
	}
	// Set up MongoDB client options
	clientOptions := options.Client().ApplyURI(common.Configuration.MongoAddressCsv)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20*time.Second))
	defer cancel()

	if common.Configuration.MongoUseSSL {
		tlsConfig := &tls.Config{}
		if common.Configuration.MongoCACertificate != "" {
			var caFile string
			if strings.HasPrefix(common.Configuration.MongoCACertificate, "/") {
				caFile = common.Configuration.MongoCACertificate
			} else {
				caFile = common.Configuration.PersistenceRootPath + common.Configuration.MongoCACertificate
			}
			serverCaCert, err := ioutil.ReadFile(caFile)
			if err != nil {
				if _, ok := err.(*os.PathError); ok {
					serverCaCert = []byte(common.Configuration.MongoCACertificate)
					err = nil
				} else {
					message := fmt.Sprintf("Failed to find mongo SSL CA file. Error: %s.", err)
					return &Error{message}
				}
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(serverCaCert)
			tlsConfig.RootCAs = caCertPool
		}

		// Please avoid using this if possible! Makes using TLS pointless
		if common.Configuration.MongoAllowInvalidCertificates {
			tlsConfig.InsecureSkipVerify = true
		}

		// Sets TLS options in options instance
		clientOptions.SetTLSConfig(tlsConfig)
	}

	for connectTime := 0; connectTime < common.Configuration.DatabaseConnectTimeout; connectTime += 10 {
		trace.Info("connect to mongo...")
		if mongoClient, err = mongo.Connect(ctx, clientOptions); err == nil {
			break
		} else {
			trace.Error("Error connecting to mongo. Error was: " + err.Error())
		}

		if connectTime == 0 && trace.IsLogging(logger.ERROR) {
			trace.Error("Failed to dial mgo. Error: " + err.Error())
		}
		if strings.HasPrefix(err.Error(), "unauthorized") ||
			strings.HasPrefix(err.Error(), "not authorized") ||
			strings.HasPrefix(err.Error(), "auth fail") ||
			strings.HasPrefix(err.Error(), "Authentication failed") {
			break
		}
		if connectTime == 0 && trace.IsLogging(logger.ERROR) {
			trace.Error("Retrying to connect to mongo")
		}

	}
	if err = mongoClient.Ping(ctx, nil); err != nil {
		message := fmt.Sprintf("Failed to ping mgo. Error: %s.", err)
		return &Error{message}
	}

	if trace.IsLogging(logger.INFO) {
		trace.Info("Connected to mongo...")
	}

	store.connected = true
	common.HealthStatus.ReconnectedToDatabase()
	if trace.IsLogging(logger.INFO) {
		trace.Info("Connected to the database")
	}
	if log.IsLogging(logger.INFO) {
		log.Info("Connected to the database")
	}

	db := mongoClient.Database(common.Configuration.MongoDbName)
	destinationsCollection := db.Collection(destinations)
	indexModel := mongo.IndexModel{Keys: bson.D{{"destination.destination-org-id", -1}}}
	if _, err = destinationsCollection.Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		message := fmt.Sprintf("Failed to create an index on %s. Error: %s", destinations, err)
		log.Error(message)
		return &Error{message}
	}

	notificationsCollection := db.Collection(notifications)
	indexModel1 := mongo.IndexModel{
		Keys: bson.D{
			{"notification.destination-org-id", -1},
			{"notification.destination-id", -1},
			{"notification.destination-type", -1},
		},
	}
	indexModel2 := mongo.IndexModel{Keys: bson.D{{"notification.resend-time", -1}, {"notification.status", -1}}}
	if _, err = notificationsCollection.Indexes().CreateMany(context.TODO(), []mongo.IndexModel{indexModel1, indexModel2}); err != nil {
		message := fmt.Sprintf("Failed to create an index on %s. Error: %s", notifications, err)
		log.Error(message)
		return &Error{message}
	}

	objectsCollection := db.Collection(objects)
	indexModel = mongo.IndexModel{Keys: bson.D{{"metadata.destination-org-id", -1}}}
	objectsCollection.Indexes().CreateOne(context.TODO(), indexModel)
	_, err = objectsCollection.Indexes().CreateOne(
		context.TODO(),
		mongo.IndexModel{
			Keys: bson.D{
				{"metadata.destination-org-id", -1},
				{"metadata.destination-policy.services.org-id", -1},
				{"metadata.destination-policy.services.service-name", -1},
			},
			Options: options.Index().SetName("syncObjects-destination-policy.services.service-id").SetSparse(true),
			// need to set index name???
		})
	if err != nil {
		message := fmt.Sprintf("Failed to create an index on %s. Error: %s", objects, err)
		log.Error(message)
		return &Error{message}
	}

	_, err = objectsCollection.Indexes().CreateOne(
		context.TODO(),
		mongo.IndexModel{
			Keys: bson.D{
				{"metadata.destination-org-id", -1},
				{"metadata.destination-policy.timestamp", -1},
			},
			Options: options.Index().SetSparse(true),
		})
	if err != nil {
		message := fmt.Sprintf("Failed to create an index on %s. Error: %s", objects, err)
		log.Error(message)
		return &Error{message}
	}
	db.Collection(acls).Indexes().CreateOne(context.TODO(), mongo.IndexModel{Keys: bson.D{{"org-id", 1}, {"acl-type", 1}}})
	gridfsBucket, err := gridfs.NewBucket(db)
	if err != nil {
		message := fmt.Sprintf("Error creating gridfs buket Error was: " + err.Error())
		log.Error(message)
		return &Error{message}
	}

	store.client = mongoClient
	store.database = db
	store.gridfsBucket = gridfsBucket
	sleepInMS = common.Configuration.MongoSleepTimeBetweenRetry

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Successfully initialized mongo driver")
	}

	return nil
}

// Stop stops the MongoStorage store
func (store *MongoStorage) Stop() {
	c := store.client
	if c != nil {
		c.Disconnect(context.TODO())
	}

}

// PerformMaintenance performs store's maintenance
func (store *MongoStorage) PerformMaintenance() {
	store.checkObjects()
}

// Cleanup erase the on disk Bolt database only for ESS and test
func (store *MongoStorage) Cleanup(isTest bool) common.SyncServiceError {
	return nil
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *MongoStorage) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	currentTime := time.Now().UTC().Format(time.RFC3339)
	query := bson.M{"$or": bson.A{
		bson.M{"status": common.NotReadyToSend},
		bson.M{"status": common.ReadyToSend},
		bson.M{"status": common.Verifying},
		bson.M{"status": common.VerificationFailed}},
		"metadata.inactive": true,
		"$and": bson.A{
			bson.M{"metadata.activation-time": bson.M{"$ne": ""}},
			bson.M{"metadata.activation-time": bson.M{"$lte": currentTime}}}}

	selector := bson.D{{"metadata", 1}}
	result := []object{}
	if err := store.fetchAll(objects, query, selector, &result); err != nil {
		return nil, err
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
	}
	return metaDatas, nil
}

// StoreObject stores an object
// If the object already exists, return the changes in its destinations list (for CSS) - return the list of deleted destinations
func (store *MongoStorage) StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	id := getObjectCollectionID(metaData)
	if !metaData.NoData && data != nil {
		if err := store.storeDataInFile(id, data); err != nil {
			return nil, err
		}
	} else if !metaData.MetaOnly {
		store.removeFile(id)
	}

	if metaData.DestinationPolicy != nil {
		metaData.DestinationPolicy.Timestamp = time.Now().UTC().UnixNano()
	}

	var dests []common.StoreDestinationStatus
	var deletedDests []common.StoreDestinationStatus
	if status == common.NotReadyToSend || status == common.ReadyToSend || status == common.Verifying || status == common.VerificationFailed {
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

	existingObject := &object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, nil, existingObject); err != nil {
		if err != mongo.ErrNoDocuments {
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
			metaData.PublicKey = existingObject.MetaData.PublicKey
			metaData.Signature = existingObject.MetaData.Signature
		}
		if metaData.DestinationPolicy != nil {
			dests = existingObject.Destinations
		}
	}

	newObj := bson.M{"$set": bson.M{"_id": id, "metadata": metaData, "status": status, "policy-received": false,
		"remaining-consumers": metaData.ExpectedConsumers,
		"remaining-receivers": metaData.ExpectedConsumers, "destinations": dests,
		"last-update": primitive.NewDateTimeFromTime(time.Now())}}

	if err := store.upsert(objects, bson.M{"_id": id, "metadata.destination-org-id": metaData.DestOrgID}, newObj); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to store an object. Error: %s.", err)}
	}

	return deletedDests, nil
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *MongoStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	result := object{}
	id := getObjectCollectionID(metaData)
	// TODO: check if selector bson.A{"destinations", 1} is correct. Or should use bson.D{{"destinations", 1}}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.A{"destinations", 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}
	}
	dests := make([]common.Destination, 0)
	for _, d := range result.Destinations {
		dests = append(dests, d.Destination)
	}
	return dests, nil
}

// GetObjectDestinationsList gets destinations that the object has to be sent to and their status
func (store *MongoStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	// TODO: check if selector bson.A{"destinations", 1} is correct. Or should use bson.D{{"destinations", 1}}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.A{"destinations", 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}
	}

	return result.Destinations, nil
}

// UpdateObjectDestinations updates object's destinations
// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
func (store *MongoStorage) UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {

	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	// bson.D{{"title", 1}, {"enrollment", 1}}
	selector := bson.D{{"metadata", 1}, {"destinations", 1}, {"last-update", 1}, {"status", 1}}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(objects, bson.M{"_id": id}, selector, &result); err != nil {
			return nil, "", nil, nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}

		dests, deletedDests, addedDests, err := createDestinations(orgID, store, result.Destinations, destinationsList)
		if err != nil {
			return nil, "", nil, nil, err
		}

		update := bson.M{
			"$set":         bson.M{"destinations": dests},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}
		if err := store.update(objects, bson.M{"_id": id, "last-update": result.LastUpdate}, update); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return nil, "", nil, nil, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
		}
		return &result.MetaData, result.Status, deletedDests, addedDests, nil
	}
	return nil, "", nil, nil, &Error{"Failed to update object's destinations."}
}

// AddObjectdestinations adds the destinations to object's destination list
// Returns the metadata, object's status, an array of added destinations after removing the overlapped destinations
func (store *MongoStorage) AddObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string, []common.StoreDestinationStatus, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	//selector: bson.D{{"title", 1}, {"enrollment", 1}}
	selector := bson.M{"metadata": 1, "destinations": 1, "last-update": 1, "status": 1}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(objects, bson.M{"_id": id}, selector, &result); err != nil {
			return nil, "", nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}

		updatedDests, addedDests, err := getDestinationsForAdd(orgID, store, result.Destinations, destinationsList)
		if err != nil {
			return nil, "", nil, err
		}

		query := bson.M{
			"$set":         bson.M{"destinations": updatedDests},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}
		if err := store.update(objects, bson.M{"_id": id, "last-update": result.LastUpdate}, query); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return nil, "", nil, &Error{fmt.Sprintf("Failed to add destinations to object's destinations list. Error: %s.", err)}
		}
		return &result.MetaData, result.Status, addedDests, nil
	}
	return nil, "", nil, &Error{"Failed to add destinations to object's destination list."}
}

// DeleteObjectdestinations deletes the destinations from object's destination list
// Returns the metadata, objects' status, an array of destinations that removed from the current destination list
func (store *MongoStorage) DeleteObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string, []common.StoreDestinationStatus, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	selector := bson.M{"metadata": 1, "destinations": 1, "last-update": 1, "status": 1}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(objects, bson.M{"_id": id}, selector, &result); err != nil {
			return nil, "", nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}

		updatedDests, deletedDests, err := getDestinationsForDelete(orgID, store, result.Destinations, destinationsList)
		if err != nil {
			return nil, "", nil, err
		}

		query := bson.M{
			"$set":         bson.M{"destinations": updatedDests},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}
		if err := store.update(objects, bson.M{"_id": id, "last-update": result.LastUpdate}, query); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return nil, "", nil, &Error{fmt.Sprintf("Failed to delete destinations from object's destinations list. Error: %s.", err)}
		}
		return &result.MetaData, result.Status, deletedDests, nil

	}
	return nil, "", nil, &Error{"Failed to delete destinations from object's destination list."}
}

// UpdateObjectDeliveryStatus changes the object's delivery status and message for the destination
// Returns true if the status is Deleted and all the destinations are in status Deleted
func (store *MongoStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) (bool, common.SyncServiceError) {
	if status == "" && message == "" {
		return false, nil
	}
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	allDeleted := true

	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(objects, bson.M{"_id": id},
			bson.M{"metadata": 1, "destinations": 1, "last-update": 1},
			&result); err != nil {
			return false, &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
		}
		found := false
		allConsumed := true
		allDeleted = true
		for i, d := range result.Destinations {
			if !found && d.Destination.DestType == destType && d.Destination.DestID == destID {
				d.Message = message
				if status != "" {
					d.Status = status
				}
				found = true
				result.Destinations[i] = d
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
			return false, &Error{"Failed to find destination."}
		}

		query := bson.M{
			"$set":         bson.M{"destinations": result.Destinations},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}
		if result.MetaData.AutoDelete && status == common.Consumed && allConsumed && result.MetaData.Expiration == "" {
			// Delete the object by setting its expiration time to one hour
			expirationTime := time.Now().Add(time.Hour * time.Duration(1)).UTC().Format(time.RFC3339)
			query = bson.M{
				"$set":         bson.M{"destinations": result.Destinations, "metadata.expiration": expirationTime},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}
		}
		if err := store.update(objects, bson.M{"_id": id, "last-update": result.LastUpdate}, query); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return false, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
		}
		return (allDeleted && status == common.Deleted), nil
	}
	return false, &Error{"Failed to update object's destinations."}
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *MongoStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(objects, bson.M{"_id": id},
			bson.M{"destinations": 1, "last-update": 1},
			&result); err != nil {
			return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
		}
		for i, d := range result.Destinations {
			d.Status = common.Delivering
			result.Destinations[i] = d
		}
		if err := store.update(objects, bson.M{"_id": id, "last-update": result.LastUpdate},
			bson.M{
				"$set":         bson.M{"destinations": result.Destinations},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
		}
		return nil
	}
	return &Error{fmt.Sprintf("Failed to update object's destinations.")}
}

// RetrieveObjectStatus finds the object and return its status
func (store *MongoStorage) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"status": 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return "", nil
		default:
			return "", &Error{fmt.Sprintf("Failed to retrieve object's status. Error: %s.", err)}
		}
	}
	return result.Status, nil
}

// RetrieveObjectRemainingConsumers finds the object and returns the number remaining consumers that
// haven't consumed the object yet
func (store *MongoStorage) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"remaining-consumers": 1}, &result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining comsumers. Error: %s.", err)}
	}
	return result.RemainingConsumers, nil
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *MongoStorage) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$inc":         bson.M{"remaining-consumers": -1},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to decrement object's remaining consumers. Error: %s.", err)}
	}
	result := object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"remaining-consumers": 1}, &result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining consumers. Error: %s.", err)}
	}
	return result.RemainingConsumers, nil
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *MongoStorage) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$inc":         bson.M{"remaining-receivers": -1},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to decrement object's remaining receivers. Error: %s.", err)}
	}
	result := object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"remaining-receivers": 1}, &result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining receivers. Error: %s.", err)}
	}
	return result.RemainingReceivers, nil
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *MongoStorage) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"metadata": 1}, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}

	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$set":         bson.M{"remaining-consumers": result.MetaData.ExpectedConsumers},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return &Error{fmt.Sprintf("Failed to reset object's remaining comsumers. Error: %s.", err)}
	}
	return nil
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed or received
// If received is true, return objects marked as received
func (store *MongoStorage) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	result := []object{}
	var query interface{}
	if received {
		query = bson.M{"$or": bson.A{
			bson.M{"status": common.CompletelyReceived},
			bson.M{"status": common.ObjReceived},
			bson.M{"status": common.ObjDeleted}},
			"metadata.destination-org-id": orgID, "metadata.object-type": objectType}
	} else {
		query = bson.M{"$or": bson.A{
			bson.M{"status": common.CompletelyReceived},
			bson.M{"status": common.ObjDeleted}},
			"metadata.destination-org-id": orgID, "metadata.object-type": objectType}
	}
	if err := store.fetchAll(objects, query, nil, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
		}
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
	}
	return metaDatas, nil
}

// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
// If received is true, return objects marked as policy received
func (store *MongoStorage) RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	var query interface{}
	if received {
		query = bson.M{
			"metadata.destination-org-id": orgID,
			"$and": bson.A{
				bson.M{"status": bson.M{"$ne": common.ObjDeleted}},
				bson.M{"metadata.destination-policy": bson.M{"$ne": nil}},
			},
		}
	} else {
		query = bson.M{
			"metadata.destination-org-id": orgID,
			"policy-received":             false,
			"$and": bson.A{
				bson.M{"status": bson.M{"$ne": common.ObjDeleted}},
				bson.M{"metadata.destination-policy": bson.M{"$ne": nil}},
			},
		}
	}
	return store.retrievePolicies(query)
}

// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
func (store *MongoStorage) RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	subquery := bson.M{
		"$elemMatch": bson.M{
			"org-id":       serviceOrgID,
			"service-name": serviceName,
		},
	}
	query := bson.M{
		"metadata.destination-org-id":          orgID,
		"metadata.destination-policy.services": subquery,
	}

	return store.retrievePolicies(query)
}

// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
func (store *MongoStorage) RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	query := bson.M{
		"metadata.destination-org-id":           orgID,
		"metadata.destination-policy.timestamp": bson.M{"$gte": since},
	}

	return store.retrievePolicies(query)
}

// RetrieveObjectsWithFilters returns the list of all the objects that meet the given conditions
func (store *MongoStorage) RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string, deleted *bool) ([]common.MetaData, common.SyncServiceError) {
	result := []object{}

	query := bson.M{
		"metadata.destination-org-id": orgID,
	}
	if destinationPolicy != nil {
		if *destinationPolicy {
			query["metadata.destination-policy"] = bson.M{"$ne": nil}
			query["metadata.destination-policy.timestamp"] = bson.M{"$gte": since}

			if dpServiceOrgID != "" && dpServiceName != "" {
				subquery := bson.M{
					"$elemMatch": bson.M{
						"org-id":       dpServiceOrgID,
						"service-name": dpServiceName,
					},
				}
				query["metadata.destination-policy.services"] = subquery
			}

			if dpPropertyName != "" {
				query["metadata.destination-policy.properties.name"] = dpPropertyName
			}
		} else {
			query["metadata.destination-policy"] = nil
		}

	}

	if objectType != "" {
		query["metadata.object-type"] = objectType
		if objectID != "" {
			query["metadata.object-id"] = objectID
		}
	}

	if destinationType != "" {
		var subquery bson.A
		if destinationID == "" {
			subquery = bson.A{
				bson.M{"metadata.destination-type": destinationType},
				bson.M{"metadata.destinations-list": bson.M{"$regex": destinationType + ":*"}},
			}
		} else {
			subquery = bson.A{
				bson.M{"metadata.destination-type": destinationType, "metadata.destination-id": destinationID},
				bson.M{"metadata.destinations-list": destinationType + ":" + destinationID},
			}
		}
		query["$or"] = subquery

	}

	if noData != nil {
		query["metadata.no-data"] = *noData
	}

	if expirationTimeBefore != "" {
		subquery := bson.M{
			"$ne":  "",
			"$lte": expirationTimeBefore,
		}
		query["metadata.expiration"] = subquery
	}

	if deleted != nil {
		query["metadata.deleted"] = *deleted
	}

	if err := store.fetchAll(objects, query, nil, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
		}
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
	}
	return metaDatas, nil

}

// RetrieveAllObjects returns the list of all the objects of the specified type
func (store *MongoStorage) RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	query := bson.M{
		"metadata.destination-org-id": orgID,
		"metadata.object-type":        objectType,
	}

	return store.retrievePolicies(query)
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination.
// Adds the new destination to the destinations lists of the relevant objects.
func (store *MongoStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	result := []object{}
	query := bson.M{"metadata.destination-org-id": orgID,
		"$or": bson.A{
			bson.M{"status": common.ReadyToSend},
			bson.M{"status": common.NotReadyToSend},
		}}

OUTER:
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchAll(objects, query, nil, &result); err != nil {
			switch err {
			case mongo.ErrNoDocuments:
				return nil, nil
			default:
				return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
			}
		}

		metaDatas := make([]common.MetaData, 0)
		for _, r := range result {
			if r.MetaData.DestinationPolicy != nil {
				continue
			}
			if (((r.MetaData.DestType == "" && len(r.MetaData.DestinationsList) == 0) || r.MetaData.DestType == destType) && (r.MetaData.DestID == "" || r.MetaData.DestID == destID)) ||
				common.StringListContains(r.MetaData.DestinationsList, fmt.Sprintf("%s:%s", destType, destID)) {
				status := common.Pending
				if r.Status == common.ReadyToSend && !r.MetaData.Inactive {
					status = common.Delivering
				}
				needToUpdate := false
				// Add destination if it doesn't exist
				if dest, err := store.RetrieveDestination(orgID, destType, destID); err == nil {
					existingDestIndex := -1
					for i, d := range r.Destinations {
						if d.Destination == *dest {
							existingDestIndex = i
							break
						}
					}
					if existingDestIndex != -1 {
						d := r.Destinations[existingDestIndex]
						if status == common.Delivering &&
							(resend == common.ResendAll || (resend == common.ResendDelivered && d.Status != common.Consumed) ||
								(resend == common.ResendUndelivered && d.Status != common.Consumed && d.Status != common.Delivered)) {
							metaDatas = append(metaDatas, r.MetaData)
							r.Destinations[existingDestIndex].Status = common.Delivering
							needToUpdate = true
						}
					} else {
						if status == common.Delivering {
							metaDatas = append(metaDatas, r.MetaData)
						}
						needToUpdate = true
						r.Destinations = append(r.Destinations, common.StoreDestinationStatus{Destination: *dest, Status: status})
					}
					if needToUpdate {
						id := createObjectCollectionID(orgID, r.MetaData.ObjectType, r.MetaData.ObjectID)
						if err := store.update(objects, bson.M{"_id": id, "last-update": r.LastUpdate},
							bson.M{
								"$set":         bson.M{"destinations": r.Destinations},
								"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
							}); err != nil {
							if IsNotFound(err) {
								time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
								continue OUTER
							}
							return nil, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
						}
					}
				}
			}
		}
		return metaDatas, nil
	}
	return nil, &Error{fmt.Sprintf("Failed to update object's destinations.")}
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
// ESS only API
func (store *MongoStorage) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *MongoStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	result := object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"metadata": 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the object. Error: %s.", err)}
		}
	}
	return &result.MetaData, nil
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *MongoStorage) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	result := &object{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.fetchOne(objects, bson.M{"_id": id}, nil, result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, "", nil
		default:
			return nil, "", &Error{fmt.Sprintf("Failed to fetch the object. Error: %s.", err)}
		}
	}
	return &result.MetaData, result.Status, nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *MongoStorage) RetrieveObjectData(orgID string, objectType string, objectID string, isTempData bool) (io.Reader, common.SyncServiceError) {
	var id string
	if isTempData {
		return nil, &Error{fmt.Sprintf("RetrieveObjectData with isTempData set true should not be called for Mongo DB.")}
	} else {
		id = createObjectCollectionID(orgID, objectType, objectID)
	}

	if store.gridfsBucket == nil {
		if bucket, err := gridfs.NewBucket(store.database); err != nil {
			return nil, err
		} else {
			store.gridfsBucket = bucket
		}
	}

	downloadStream, err := store.gridfsBucket.OpenDownloadStreamByName(id)
	if err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		case gridfs.ErrFileNotFound:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to find file to read the data. Error: %s.", err)}
		}
	}
	return downloadStream, nil
}

// CloseDataReader closes the data reader if necessary
func (store *MongoStorage) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	switch v := dataReader.(type) {
	case *gridfs.DownloadStream:
		v.Close()
		return nil
	default:
		return nil
	}
}

// ReadObjectData returns the object data with the specified parameters
func (store *MongoStorage) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	fileHandle, err := store.openFile(id)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, true, 0, &common.NotFound{}
		}
		return nil, true, 0, &Error{fmt.Sprintf("Failed to open file to read the data. Error: %s.", err)}
	}

	offset64 := int64(offset)
	if offset64 >= fileHandle.GetFile().Length {
		fileHandle.Close()
		return make([]byte, 0), true, 0, nil
	}

	b := make([]byte, fileHandle.GetFile().Length)
	_, err = fileHandle.Read(b)
	if err != nil {
		fileHandle.Close()
		return nil, true, 0, &Error{fmt.Sprintf("Failed to read the data. Error: %s.", err)}
	}

	if err = fileHandle.Close(); err != nil {
		return nil, true, 0, &Error{fmt.Sprintf("Failed to close the file. Error: %s.", err)}
	}

	br := bytes.NewReader(b)
	_, err = br.Seek(offset64, 0)
	if err != nil {
		return nil, true, 0, &Error{fmt.Sprintf("Failed to read the data. Error: %s.", err)}
	}

	s := int64(size)
	if s > fileHandle.GetFile().Length-offset64 {
		s = fileHandle.GetFile().Length - offset64
	}

	ret := make([]byte, s)
	n, err := br.Read(ret)

	if err != nil {
		return nil, true, 0, &Error{fmt.Sprintf("Failed to read the data. Error: %s.", err)}
	}

	eof := false
	if fileHandle.GetFile().Length-offset64 == int64(n) {
		eof = true
	}

	return ret, eof, n, nil
}

// StoreObjectData stores object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *MongoStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"status": 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return false, nil
		default:
			return false, &Error{fmt.Sprintf("Failed to store the data. Error: %s.", err)}
		}
	}

	// If it is called by dataVerifier, the status is verifying. The object status will not changed to "ready".
	// This is because at this moment, the data is not yet verified.
	if result.Status == common.NotReadyToSend {
		store.UpdateObjectStatus(orgID, objectType, objectID, common.ReadyToSend)
	}
	if result.Status == common.NotReadyToSend || result.Status == common.ReadyToSend || result.Status == common.Verifying {
		newID := store.getInstanceID()
		if err := store.update(objects, bson.M{"_id": id},
			bson.M{
				"$set":         bson.M{"metadata.data-id": newID, "metadata.instance-id": newID},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			return false, &Error{fmt.Sprintf("Failed to set instance id. Error: %s.", err)}
		}
	}
	if err := store.copyDataToFile(id, dataReader); err != nil {
		return false, err
	}

	fileInfo, err := store.getFileInfo(id)
	if err != nil {
		return false, &Error{fmt.Sprintf("Failed to get mongo file information. Error: %s.", err)}
	}

	// Update object size
	if err := store.update(objects, bson.M{"_id": id}, bson.M{"$set": bson.M{"metadata.object-size": fileInfo.Length}}); err != nil {
		return false, &Error{fmt.Sprintf("Failed to update object's size. Error: %s.", err)}
	}

	return true, nil
}

func (store *MongoStorage) StoreObjectTempData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	return false, &Error{fmt.Sprintf("StoreObjectTempData should not be called for Mongo DB.")}
}

// Removes all the temporary chunk files from Mongo. Depends on RetrieveObjectTempData putting the fileHandle of the temp file into the map
// The verification step will be done by the CSS that opened all the files so should have the correct mapping
func (store *MongoStorage) RemoveObjectTempData(orgID string, objectType string, objectID string) common.SyncServiceError {

	if trace.IsLogging(logger.TRACE) {
		trace.Trace(fmt.Sprintf("RemoveObjectTempData for org - %s, type - %s, id - %s", orgID, objectType, objectID))
	}

	metaData, err := store.RetrieveObject(orgID, objectType, objectID)
	if err != nil || metaData == nil {
		return &Error{fmt.Sprintf("Error in retrieving object metadata.\n")}
	}

	var offset int64 = 0
	chunkNumber := 1

	// Will be 0 if data was uploaded with streaming
	if metaData.UploadChunkSize > 0 {

		for offset < metaData.ObjectSize {

			id := createTempObjectCollectionID(orgID, objectType, objectID, chunkNumber)
			if trace.IsLogging(logger.TRACE) {
				trace.Trace(fmt.Sprintf("RemoveObjectTempData for org - %s, type - %s, id - %s, chunkNum - %d", orgID, objectType, objectID, chunkNumber))
			}
			fileHandle, _ := store.retrieveObjectTempData(id)

			if fileHandle != nil {
				fileHandle.Close()
				//store.deleteFileHandle(id)

				//Don't return on errors
				store.removeFile(id)
			}

			chunkNumber += 1
			offset += metaData.UploadChunkSize
		}
	}

	return nil
}

// For mongo implementation, each chunk is saved to a separate document. In this method, we have to combine all those documents into a MultiReader
func (store *MongoStorage) RetrieveObjectTempData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {

	if trace.IsLogging(logger.TRACE) {
		trace.Trace(fmt.Sprintf("RetrieveObjectTempData for org - %s, type - %s, id - %s", orgID, objectType, objectID))
	}
	readers := make([]io.Reader, 0)
	metaData, err := store.RetrieveObject(orgID, objectType, objectID)
	if err != nil || metaData == nil {
		return nil, &Error{fmt.Sprintf("Error in retrieving object metadata.\n")}
	}

	var offset int64 = 0
	chunkNumber := 1

	// Will be 0 if data was uploaded with streaming
	if metaData.UploadChunkSize > 0 {

		for offset < metaData.ObjectSize {

			id := createTempObjectCollectionID(orgID, objectType, objectID, chunkNumber)
			if trace.IsLogging(logger.TRACE) {
				trace.Trace(fmt.Sprintf("RetrieveObjectTempData for org - %s, type - %s, id - %s, chunkNum - %d", orgID, objectType, objectID, chunkNumber))
			}
			fileHandle, err := store.retrieveObjectTempData(id)
			if err != nil {
				return nil, &Error{fmt.Sprintf("Error in retrieving objects chunk data. Error: %s.\n", err)}
			}

			if fileHandle != nil {
				readers = append(readers, fileHandle)
			}

			chunkNumber += 1
			offset += metaData.UploadChunkSize
		}
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace(fmt.Sprintf("returning %d file readers in the MultiReader from RetrieveObjectTempData", len(readers)))
	}
	rr := io.MultiReader(readers...)
	return rr, nil

}

func (store *MongoStorage) retrieveObjectTempData(id string) (*gridfs.DownloadStream, common.SyncServiceError) {
	fileHandle, err := store.openFile(id)
	if err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to open file to read the data. Error: %s.", err)}
		}
	}
	return fileHandle, nil
}

// AppendObjectData appends a chunk of data to the object's data. Design is that each chunk will be written to a new document in mongo and then combined
// into final document by user caller RetrieveObjectTempData during verification step (or just copied if no verification necessary)
func (store *MongoStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader,
	dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool, isTempData bool) (bool, common.SyncServiceError) {

	var n int
	var err error
	var data []byte
	if dataLength > 0 {
		data = make([]byte, dataLength)
		n, err = dataReader.Read(data)
	} else {
		data, err = ioutil.ReadAll(dataReader)
		n = len(data)
	}
	if err != nil && err != io.EOF {
		return isLastChunk, &Error{fmt.Sprintf("Failed to read the data from the dataReader. Error: %s.", err)}
	}
	if uint32(n) != dataLength && dataLength > 0 {
		return isLastChunk, &Error{fmt.Sprintf("Failed to read all the data from the dataReader. Read %d instead of %d.", n, dataLength)}
	}

	updatedLastChunk := isLastChunk

	var n_int64 int64 = int64(n)

	// Set the chunk size which will be the amount of data sent in first chunk. Works even if all data fits in one chunk. Needs to be saved for when temp documents
	// need to be moved to final mongo document location to compute how many chunks were received for this object. Also need object size stored in metaData here
	if isFirstChunk {
		err := store.setUploadDataInfo(orgID, objectType, objectID, n_int64, total)
		if err != nil {
			return isLastChunk, &Error{fmt.Sprintf("Failed to set the upload chunk size. Error: %s", err)}
		}
	}

	// Figure out which chunk this is by looking at offset + length of data
	var chunkNumber int
	if (offset + n_int64) < total {
		chunkNumber = (int(offset) + n) / n
	} else {
		updatedLastChunk = true

		if offset == 0 {
			// Special case if file fits in a single chunk
			chunkNumber = 1
		} else {

			// This is the last chunk so to figure out what chunk number it is, need to know the chunk size and the total object size
			chunkSize, err := store.getUploadDataChunkSize(orgID, objectType, objectID)

			if err != nil {
				return isLastChunk, &Error{fmt.Sprintf("Failed to read the upload chunk size. Error: %s.", err)}
			}

			chunkNumber = (int)(total / chunkSize)
			if total%chunkSize != 0 {
				chunkNumber += 1
			}
		}
	}

	// In Mongo implementation, all data passed to this function is to be stored in temp data
	id := createTempObjectCollectionID(orgID, objectType, objectID, chunkNumber)

	//var fileHandle *fileHandle
	store.removeFile(id)
	br := bytes.NewReader(data)
	if err = store.createFile(id, br); err != nil {
		return isLastChunk, err
	}

	if updatedLastChunk {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Model file completely written; set updatedLastChunk to %t\n", updatedLastChunk)
		}
	}

	//store.deleteFileHandle(id)
	//err = fileHandle.Close()
	if err != nil {
		return updatedLastChunk, &Error{fmt.Sprintf("Failed to close the file. Error: %s.", err)}
	}

	return updatedLastChunk, nil
}

// Handles storing the upload data chunk size and the total size
func (store *MongoStorage) setUploadDataInfo(orgID string, objectType string, objectID string, chunkSize int64, totalSize int64) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id}, bson.M{"$set": bson.M{"metadata.upload-chunk-size": chunkSize, "metadata.object-size": totalSize}}); err != nil {
		return &Error{fmt.Sprintf("Failed to set uploadDataChunkSize. Error: %s.", err)}
	}
	return nil
}

// Handles retrieving the upload data chunk size
func (store *MongoStorage) getUploadDataChunkSize(orgID string, objectType string, objectID string) (int64, common.SyncServiceError) {

	metaData, err := store.RetrieveObject(orgID, objectType, objectID)
	if err != nil || metaData == nil || metaData.UploadChunkSize == 0 {
		return 0, &Error{fmt.Sprintf("Error in getUploadDataChunkSize. Failed to find upload chunk size.\n")}
	}

	return metaData.UploadChunkSize, nil
}

// Handles the last data chunk when no data verification needed
func (store *MongoStorage) HandleObjectInfoForLastDataChunk(orgID string, objectType string, objectID string, isTempData bool, dataSize int64) (bool, common.SyncServiceError) {
	if isTempData {
		return false, nil
	}

	id := createObjectCollectionID(orgID, objectType, objectID)

	result := object{}
	if err := store.fetchOne(objects, bson.M{"_id": id}, bson.M{"status": 1}, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return false, nil
		default:
			return false, &Error{fmt.Sprintf("Failed to store the data. Error: %s.", err)}
		}
	}

	if result.Status == common.NotReadyToSend {
		store.UpdateObjectStatus(orgID, objectType, objectID, common.ReadyToSend)
	}
	if result.Status == common.NotReadyToSend || result.Status == common.ReadyToSend {
		newID := store.getInstanceID()
		if err := store.update(objects, bson.M{"_id": id},
			bson.M{
				"$set":         bson.M{"metadata.data-id": newID, "metadata.instance-id": newID},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			return false, &Error{fmt.Sprintf("Failed to set instance id. Error: %s.", err)}
		}
	}

	// Update object size
	if err := store.update(objects, bson.M{"_id": id}, bson.M{"$set": bson.M{"metadata.object-size": dataSize}}); err != nil {
		return false, &Error{fmt.Sprintf("Failed to update object's size. Error: %s.", err)}
	}

	return true, nil
}

// UpdateObjectStatus updates object's status
func (store *MongoStorage) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$set":         bson.M{"status": status},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return &Error{fmt.Sprintf("Failed to update object's status. Error: %s.", err)}
	}
	return nil
}

// UpdateObjectSourceDataURI updates object's source data URI
func (store *MongoStorage) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	return nil
}

// MarkObjectDeleted marks the object as deleted
func (store *MongoStorage) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$set":         bson.M{"status": common.ObjDeleted, "metadata.deleted": true},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return &Error{fmt.Sprintf("Failed to mark object as deleted. Error: %s.", err)}
	}
	return nil
}

// MarkDestinationPolicyReceived marks an object's destination policy as having been received
func (store *MongoStorage) MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{
			"$set":         bson.M{"policy-received": true},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return &Error{fmt.Sprintf("Failed to mark an object's destination policy as received. Error: %s", err)}
	}
	return nil
}

// ActivateObject marks object as active
func (store *MongoStorage) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.update(objects, bson.M{"_id": id},
		bson.M{"$set": bson.M{"metadata.inactive": false},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}); err != nil {
		return &Error{fmt.Sprintf("Failed to mark object as active. Error: %s.", err)}
	}
	return nil
}

// DeleteStoredObject deletes the object
func (store *MongoStorage) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	t := time.Date(0001, 1, 1, 00, 00, 00, 00, time.UTC)
	return store.deleteObject(orgID, objectType, objectID, t)
}

// DeleteStoredData deletes the object's data
func (store *MongoStorage) DeleteStoredData(orgID string, objectType string, objectID string, isTempData bool) common.SyncServiceError {
	var id string
	if isTempData {
		// Make sure we have all the temp data by calling RetrieveObjectTempData here
		_, err := store.RetrieveObjectTempData(orgID, objectType, objectID)
		if err == nil {
			return store.RemoveObjectTempData(orgID, objectType, objectID)
		} else {
			return err
		}
	} else {
		id = createObjectCollectionID(orgID, objectType, objectID)
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Deleting object's data %s\n", id)
		}
		if err := store.removeFile(id); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in DeleteStoredData: failed to delete data file. Error: %s\n", err)
			}
			return err
		}
	}

	return nil
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *MongoStorage) CleanObjects() common.SyncServiceError {
	// ESS only function
	return nil
}

// GetNumberOfStoredObjects returns the number of objects received from the application that are
// currently stored in this node's storage
func (store *MongoStorage) GetNumberOfStoredObjects() (uint32, common.SyncServiceError) {
	query := bson.M{
		"$or": bson.A{
			bson.M{"status": common.ReadyToSend},
			bson.M{"status": common.NotReadyToSend},
			bson.M{"status": common.Verifying},
			bson.M{"status": common.VerificationFailed},
		}}
	return store.count(objects, query)
}

// AddWebhook stores a webhook for an object type
func (store *MongoStorage) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Adding a webhook for %s\n", id)
	}
	result := &webhookObject{}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(webhooks, bson.M{"_id": id}, nil, &result); err != nil {
			if err == mongo.ErrNoDocuments {
				result.Hooks = make([]string, 0)
				result.Hooks = append(result.Hooks, url)
				result.ID = id
				if err = store.insert(webhooks, result); err != nil {
					if mongo.IsDuplicateKeyError(err) {
						time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
						continue
					}
					return &Error{fmt.Sprintf("Failed to insert a webhook. Error: %s.", err)}
				}
				return nil
			}
			return &Error{fmt.Sprintf("Failed to add a webhook. Error: %s.", err)}
		}

		// Don't add the webhook if it already is in the list
		for _, hook := range result.Hooks {
			if url == hook {
				return nil
			}
		}
		result.Hooks = append(result.Hooks, url)
		if err := store.update(webhooks, bson.M{"_id": id, "last-update": result.LastUpdate},
			bson.M{
				"$set":         bson.M{"hooks": result.Hooks},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return &Error{fmt.Sprintf("Failed to add a webhook. Error: %s.", err)}
		}
		return nil
	}
	return &Error{fmt.Sprintf("Failed to add a webhook.")}
}

// DeleteWebhook deletes a webhook for an object type
func (store *MongoStorage) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting a webhook for %s\n", id)
	}
	result := &webhookObject{}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(webhooks, bson.M{"_id": id}, nil, &result); err != nil {
			return &Error{fmt.Sprintf("Failed to delete a webhook. Error: %s.", err)}
		}
		deleted := false
		for i, hook := range result.Hooks {
			if strings.EqualFold(hook, url) {
				result.Hooks[i] = result.Hooks[len(result.Hooks)-1]
				result.Hooks = result.Hooks[:len(result.Hooks)-1]
				deleted = true
				break
			}
		}
		if !deleted {
			return nil
		}
		if err := store.update(webhooks, bson.M{"_id": id, "last-update": result.LastUpdate},
			bson.M{
				"$set":         bson.M{"hooks": result.Hooks},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return &Error{fmt.Sprintf("Failed to delete a webhook. Error: %s.", err)}
		}
		return nil
	}
	return &Error{fmt.Sprintf("Failed to delete a webhook.")}
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *MongoStorage) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving a webhook for %s\n", id)
	}
	result := &webhookObject{}
	if err := store.fetchOne(webhooks, bson.M{"_id": id}, nil, &result); err != nil {
		return nil, err
	}
	if len(result.Hooks) == 0 {
		return nil, &NotFound{"No webhooks"}
	}
	return result.Hooks, nil
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *MongoStorage) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	result := []destinationObject{}
	var err error

	if orgID == "" {
		if destType == "" {
			err = store.fetchAll(destinations, bson.M{}, nil, &result)
		} else {
			err = store.fetchAll(destinations, bson.M{"destination.destination-type": destType}, nil, &result)
		}
	} else {
		if destType == "" {
			err = store.fetchAll(destinations, bson.M{"destination.destination-org-id": orgID}, nil, &result)
		} else {
			err = store.fetchAll(destinations, bson.M{"destination.destination-org-id": orgID, "destination.destination-type": destType}, nil, &result)
		}
	}
	if err != nil && err != mongo.ErrNoDocuments {
		return nil, &Error{fmt.Sprintf("Failed to fetch the destinations. Error: %s.", err)}
	}

	dests := make([]common.Destination, len(result))
	for i, r := range result {
		dests[i] = r.Destination
	}
	return dests, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *MongoStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	result := destinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.fetchOne(destinations, bson.M{"_id": id}, nil, &result); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// StoreDestination stores the destination
func (store *MongoStorage) StoreDestination(destination common.Destination) common.SyncServiceError {
	id := getDestinationCollectionID(destination)
	filter := bson.D{{"_id", id}, {"destination.destination-org-id", destination.DestOrgID}}
	newObject := bson.D{{"$set", bson.D{{"_id", id}, {"destination", destination}, {"last-ping-time", primitive.NewDateTimeFromTime(time.Now())}}}}
	err := store.upsert(destinations, filter, newObject)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to store a destination. Error: %s.", err)}
	}
	return nil
}

// DeleteDestination deletes the destination
func (store *MongoStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.removeAll(destinations, bson.M{"_id": id}); err != nil {
		return &Error{fmt.Sprintf("Failed to delete destination. Error: %s.", err)}
	}
	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *MongoStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	id := getDestinationCollectionID(destination)
	err := store.update(destinations,
		bson.M{"_id": id},
		bson.M{"$currentDate": bson.M{"last-ping-time": bson.M{"$type": "date"}}},
	)
	if err != nil {
		if IsNotFound(err) {
			return err
		}
		return &Error{fmt.Sprintf("Failed to update the last ping time for destination. Error: %s\n", err)}
	}

	return nil
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *MongoStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {
	query := bson.M{"last-ping-time": bson.M{"$lte": lastTimestamp}}
	selector := bson.M{"destination": 1}
	dests := []destinationObject{}
	if err := store.fetchAll(destinations, query, selector, &dests); err != nil {
		if err != mongo.ErrNoDocuments && log.IsLogging(logger.ERROR) {
			log.Error("Error in mongoStorage.RemoveInactiveDestinations: failed to remove inactive destinations. Error: %s\n", err)
		}
		return
	}
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Removing inactive destinations")
	}
	for _, d := range dests {
		if err := store.DeleteNotificationRecords(d.Destination.DestOrgID, "", "", d.Destination.DestType, d.Destination.DestID); err != nil &&
			err != mongo.ErrNoDocuments && log.IsLogging(logger.ERROR) {
			log.Error("Error in mongoStorage.RemoveInactiveDestinations: failed to remove notifications for inactive destinations. Error: %s\n", err)
		}
		if err := store.DeleteDestination(d.Destination.DestOrgID, d.Destination.DestType, d.Destination.DestID); err != nil &&
			err != mongo.ErrNoDocuments && log.IsLogging(logger.ERROR) {
			log.Error("Error in mongoStorage.RemoveInactiveDestinations: failed to remove inactive destination. Error: %s\n", err)
		}
	}
}

// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
func (store *MongoStorage) GetNumberOfDestinations() (uint32, common.SyncServiceError) {
	return store.count(destinations, nil)
}

// RetrieveDestinationProtocol retrieves the communication protocol for the destination
func (store *MongoStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	result := destinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.fetchOne(destinations, bson.M{"_id": id}, nil, &result); err != nil {
		return "", &Error{fmt.Sprintf("Failed to fetch the destination. Error: %s.", err)}
	}
	return result.Destination.Communication, nil
}

// RetrieveDestination retrieves a destination
func (store *MongoStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	result := destinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.fetchOne(destinations, bson.M{"_id": id}, nil, &result); err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, &Error{fmt.Sprintf("Failed to fetch the destination. Error: %s.", err)}
		}
		return nil, &NotFound{fmt.Sprintf(" The destination %s:%s does not exist", destType, destID)}
	}
	return &result.Destination, nil
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *MongoStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	notificationRecords := []notificationObject{}
	query := bson.M{"$or": bson.A{
		bson.M{"notification.status": common.Update},
		bson.M{"notification.status": common.UpdatePending},
		bson.M{"notification.status": common.Updated},
		bson.M{"notification.status": common.ReceivedByDestination},
		bson.M{"notification.status": common.ConsumedByDestination},
		bson.M{"notification.status": common.Error}},
		"notification.destination-org-id": orgID,
		"notification.destination-id":     destID,
		"notification.destination-type":   destType}

	if err := store.fetchAll(notifications, query, nil, &notificationRecords); err != nil && err != mongo.ErrNoDocuments {
		return nil, &Error{fmt.Sprintf("Failed to fetch the notifications. Error: %s.", err)}
	}

	var status string
	objectStatuses := make([]common.ObjectStatus, 0)
	for _, n := range notificationRecords {
		switch n.Notification.Status {
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
		objectStatus := common.ObjectStatus{OrgID: orgID, ObjectType: n.Notification.ObjectType, ObjectID: n.Notification.ObjectID, Status: status}
		objectStatuses = append(objectStatuses, objectStatus)
	}
	return objectStatuses, nil
}

// RetrieveAllObjectsAndUpdateDestinationListForDestination retrieves objects that are in use on a given node and the destination status
func (store *MongoStorage) RetrieveAllObjectsAndUpdateDestinationListForDestination(destOrgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError) {
	result := []object{}

	query := bson.M{}
	subquery := bson.M{
		"$elemMatch": bson.M{
			"destination.destination-org-id": destOrgID,
			"destination.destination-type":   destType,
			"destination.destination-id":     destID,
		},
	}
	query["destinations"] = subquery

	if err := store.fetchAll(objects, query, nil, &result); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the objects for destination %s %s %s from storage. Error: %s.", destOrgID, destType, destID, err)}
		}
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
		updatedDestinationList := make([]common.StoreDestinationStatus, 0)
		for _, dest := range r.Destinations {
			if dest.Destination.DestOrgID == destOrgID && dest.Destination.DestType == destType && dest.Destination.DestID == destID {
			} else {
				updatedDestinationList = append(updatedDestinationList, dest)
			}
		}

		query := bson.M{
			"$set":         bson.M{"destinations": updatedDestinationList},
			"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
		}
		if err := store.update(objects, bson.M{"_id": r.ID, "last-update": r.LastUpdate}, query); err != nil {
			if IsNotFound(err) {
				continue
			}
			emptyMeta := make([]common.MetaData, 0)
			return emptyMeta, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
		}

	}
	return metaDatas, nil
}

// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
func (store *MongoStorage) RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError) {
	removedDestinationPolicyServices := []common.ServiceID{}
	return nil, removedDestinationPolicyServices, nil
}

// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
func (store *MongoStorage) UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError {
	return nil
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *MongoStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	id := getNotificationCollectionID(&notification)
	if notification.ResendTime == 0 {
		resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
		notification.ResendTime = resendTime
	}
	n := bson.M{"$set": bson.M{"_id": id, "notification": notification}}
	err := store.upsert(notifications,
		bson.M{
			"_id":                             id,
			"notification.destination-org-id": notification.DestOrgID,
			"notification.destination-id":     notification.DestID,
			"notification.destination-type":   notification.DestType,
		},
		n)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to update notification record. Error: %s.", err)}
	}

	no := notificationObject{}
	_ = store.fetchOne(notifications, bson.M{"_id": id}, nil, &no)
	return nil
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *MongoStorage) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {
	id := getNotificationCollectionID(&notification)
	resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
	if err := store.update(notifications, bson.M{"_id": id}, bson.M{"$set": bson.M{"notification.resend-time": resendTime}}); err != nil {
		return &Error{fmt.Sprintf("Failed to update notification resend time. Error: %s.", err)}
	}
	return nil
}

// RetrieveNotificationRecord retrieves notification
func (store *MongoStorage) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {
	id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
	result := notificationObject{}
	if err := store.fetchOne(notifications, bson.M{"_id": id}, nil, &result); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, &Error{fmt.Sprintf("Failed to fetch the notification. Error: %s.", err)}
	}
	return &result.Notification, nil
}

// DeleteNotificationRecords deletes notification records to an object
func (store *MongoStorage) DeleteNotificationRecords(orgID string, objectType string, objectID string, destType string, destID string) common.SyncServiceError {
	var err error
	if objectType != "" && objectID != "" {
		if destType != "" && destID != "" {
			id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
			err = store.removeAll(notifications, bson.M{"_id": id})
		} else {
			err = store.removeAll(notifications,
				bson.M{"notification.destination-org-id": orgID, "notification.object-type": objectType,
					"notification.object-id": objectID})
		}
	} else {
		err = store.removeAll(notifications,
			bson.M{"notification.destination-org-id": orgID, "notification.destination-type": destType,
				"notification.destination-id": destID})
	}

	if err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to delete notification records. Error: %s.", err)}
	}
	return nil
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *MongoStorage) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError) {
	result := []notificationObject{}
	var query bson.M
	if destType == "" && destID == "" {
		currentTime := primitive.NewDateTimeFromTime(time.Now())

		query = bson.M{"$or": bson.A{
			bson.M{"notification.status": common.Getdata},
			bson.M{
				"notification.resend-time": bson.M{"$lte": currentTime},
				"$or": bson.A{
					bson.M{"notification.status": common.Update},
					bson.M{"notification.status": common.Received},
					bson.M{"notification.status": common.Consumed},
					bson.M{"notification.status": common.Getdata},
					bson.M{"notification.status": common.Delete},
					bson.M{"notification.status": common.Deleted},
					bson.M{"notification.status": common.Error}}}}}
	} else {
		if retrieveReceived {
			query = bson.M{"$or": bson.A{
				bson.M{"notification.status": common.Update},
				bson.M{"notification.status": common.Updated},
				bson.M{"notification.status": common.Received},
				bson.M{"notification.status": common.Consumed},
				bson.M{"notification.status": common.Getdata},
				bson.M{"notification.status": common.ReceivedByDestination},
				bson.M{"notification.status": common.Delete},
				bson.M{"notification.status": common.Deleted}},
				"notification.destination-org-id": orgID,
				"notification.destination-id":     destID,
				"notification.destination-type":   destType}
		} else {
			query = bson.M{"$or": bson.A{
				bson.M{"notification.status": common.Update},
				bson.M{"notification.status": common.Received},
				bson.M{"notification.status": common.Consumed},
				bson.M{"notification.status": common.Getdata},
				bson.M{"notification.status": common.Delete},
				bson.M{"notification.status": common.Deleted}},
				"notification.destination-org-id": orgID,
				"notification.destination-id":     destID,
				"notification.destination-type":   destType}
		}
	}
	if err := store.fetchAll(notifications, query, nil, &result); err != nil && err != mongo.ErrNoDocuments {
		return nil, &Error{fmt.Sprintf("Failed to fetch the notifications. Error: %s.", err)}
	}

	notifications := make([]common.Notification, 0)
	for _, n := range result {
		notifications = append(notifications, n.Notification)
	}
	return notifications, nil
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *MongoStorage) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	result := []notificationObject{}
	var query bson.M

	if destType == "" && destID == "" {
		query = bson.M{"$or": bson.A{
			bson.M{"notification.status": common.UpdatePending},
			bson.M{"notification.status": common.ReceivedPending},
			bson.M{"notification.status": common.ConsumedPending},
			bson.M{"notification.status": common.DeletePending},
			bson.M{"notification.status": common.DeletedPending}},
			"notification.destination-org-id": orgID}
	} else {
		query = bson.M{"$or": bson.A{
			bson.M{"notification.status": common.UpdatePending},
			bson.M{"notification.status": common.ReceivedPending},
			bson.M{"notification.status": common.ConsumedPending},
			bson.M{"notification.status": common.DeletePending},
			bson.M{"notification.status": common.DeletedPending}},
			"notification.destination-org-id": orgID,
			"notification.destination-id":     destID,
			"notification.destination-type":   destType}
	}
	if err := store.fetchAll(notifications, query, nil, &result); err != nil && err != mongo.ErrNoDocuments {
		return nil, &Error{fmt.Sprintf("Failed to fetch the notifications. Error: %s.", err)}
	}

	notifications := make([]common.Notification, 0)
	for _, n := range result {
		notifications = append(notifications, n.Notification)
	}
	return notifications, nil
}

// InsertInitialLeader inserts the initial leader document if the collection is empty
func (store *MongoStorage) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {
	doc := leaderDocument{ID: 1, UUID: leaderID, HeartbeatTimeout: common.Configuration.LeadershipTimeout, Version: 1}
	err := store.insert(leader, doc)

	if err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return false, &Error{fmt.Sprintf("Failed to insert document into syncLeaderElection collection. Error: %s\n", err)}
		}
		return false, nil
	}

	return true, nil
}

// LeaderPeriodicUpdate does the periodic update of the leader document by the leader
func (store *MongoStorage) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {
	var err common.SyncServiceError
	for i := 0; i < maxUpdateTries; i++ {
		err = store.update(leader,
			bson.M{"_id": 1, "uuid": leaderID},
			bson.M{"$currentDate": bson.M{"last-heartbeat-ts": bson.M{"$type": "date"}}},
		)
		if err != nil {
			if IsNotFound(err) {
				time.Sleep(time.Duration(sleepInMS) * time.Millisecond)
				continue
			}
			return false, &Error{fmt.Sprintf("Failed to update the document in the syncLeaderElection collection. Error: %s\n", err)}
		}
		return true, nil
	}

	if err != nil {
		return false, nil
	}
	return true, nil
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *MongoStorage) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	doc := leaderDocument{}
	err := store.fetchOne(leader, bson.M{"_id": 1}, nil, &doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", 0, time.Now(), 0, &NotFound{}
		}
		return "", 0, time.Now(), 0, &Error{fmt.Sprintf("Failed to fetch the document in the syncLeaderElection collection. Error: %s", err)}
	}
	return doc.UUID, doc.HeartbeatTimeout, doc.LastHeartbeatTS, doc.Version, nil
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *MongoStorage) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {
	err := store.update(leader,
		bson.M{"_id": 1, "version": version},
		bson.M{
			"$currentDate": bson.M{"last-heartbeat-ts": bson.M{"$type": "date"}},
			"$set": bson.M{
				"uuid":              leaderID,
				"heartbeat-timeout": common.Configuration.LeadershipTimeout,
				"version":           version + 1,
			},
		},
	)
	if err != nil {
		if !IsNotFound(err) {
			// Only complain if someone else didn't steal the leadership
			return false, &Error{fmt.Sprintf("Failed to update the document in the syncLeaderElection collection. Error: %s\n", err)}
		}
		return false, nil
	}
	return true, nil
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *MongoStorage) ResignLeadership(leaderID string) common.SyncServiceError {
	timestamp := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	err := store.update(leader,
		bson.M{"_id": 1, "uuid": leaderID},
		bson.M{
			"$set": bson.M{
				"last-heartbeat-ts": timestamp,
			},
		},
	)
	if err != nil && !IsNotFound(err) {
		return &Error{fmt.Sprintf("Failed to update the document in the syncLeaderElection collection. Error: %s\n", err)}
	}

	return nil
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *MongoStorage) RetrieveTimeOnServer() (time.Time, error) {
	/*
		> db.runCommand( { isMaster: 1 } )
		{
		ismaster: true,
		topologyVersion: {
			processId: ObjectId('66425e524a6e53adeab8efca'),
			counter: Long('0')
		},
		maxBsonObjectSize: 16777216,
		maxMessageSizeBytes: 48000000,
		maxWriteBatchSize: 100000,
		localTime: ISODate('2024-05-15T19:13:13.097Z'),
		logicalSessionTimeoutMinutes: 30,
		connectionId: 523,
		minWireVersion: 0,
		maxWireVersion: 17,
		readOnly: false,
		ok: 1
		}
	*/
	result := isMasterResult{}
	err := store.run(bson.D{{"isMaster", 1}}, &result)
	if err == nil && !result.OK {
		err = &Error{"Failed running isMaster command on MongoDB server"}
	}
	return result.LocalTime, err
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *MongoStorage) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {
	mg := bson.M{"$set": bson.M{"_id": orgID, "group-name": messagingGroup, "last-update": primitive.NewDateTimeFromTime(time.Now())}}
	err := store.upsert(messagingGroups, bson.M{"_id": orgID}, mg)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to store organization's messaging group. Error: %s.", err)}
	}
	return nil
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *MongoStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	if err := store.removeAll(messagingGroups, bson.M{"_id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	return nil
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *MongoStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	result := messagingGroupObject{}
	if err := store.fetchOne(messagingGroups, bson.M{"_id": orgID}, nil, &result); err != nil {
		if err != mongo.ErrNoDocuments {
			return "", err
		}
		return "", nil
	}
	return result.GroupName, nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *MongoStorage) RetrieveUpdatedMessagingGroups(timeToCheck time.Time) ([]common.MessagingGroup,
	common.SyncServiceError) {

	result := []messagingGroupObject{}
	if err := store.fetchAll(messagingGroups, bson.M{"last-update": bson.M{"$gte": timeToCheck}}, nil, &result); err != nil {
		return nil, err
	}
	groups := make([]common.MessagingGroup, 0)
	for _, group := range result {
		groups = append(groups, common.MessagingGroup{OrgID: group.ID, GroupName: group.GroupName})
	}
	return groups, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *MongoStorage) DeleteOrganization(orgID string) common.SyncServiceError {
	if err := store.DeleteOrgToMessagingGroup(orgID); err != nil {
		return err
	}

	if err := store.removeAll(destinations, bson.M{"destination.destination-org-id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to delete destinations. Error: %s.", err)}
	}

	if err := store.removeAll(notifications, bson.M{"notification.destination-org-id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to delete notifications. Error: %s.", err)}
	}

	if err := store.removeAll(acls, bson.M{"org-id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to delete ACLs. Error: %s.", err)}
	}

	type idstruct struct {
		ID string `bson:"_id"`
	}
	results := []idstruct{}
	if err := store.fetchAll(objects, bson.M{"metadata.destination-org-id": orgID}, bson.M{"_id": 1}, &results); err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to fetch objects to delete. Error: %s.", err)}
	}
	for _, result := range results {
		store.removeFile(result.ID)
	}

	if err := store.removeAll(objects, bson.M{"metadata.destination-org-id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return &Error{fmt.Sprintf("Failed to delete objects. Error: %s.", err)}
	}

	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *MongoStorage) IsConnected() bool {
	return store.connected
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *MongoStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {

	timestamp := time.Now()
	objectBson := bson.M{"$set": bson.M{"_id": org.OrgID, "org": org, "last-update": primitive.NewDateTimeFromTime(timestamp)}}
	err := store.upsert(organizations, bson.M{"_id": org.OrgID}, objectBson)
	if err != nil {
		return timestamp, &Error{fmt.Sprintf("Failed to store organization's info. Error: %s.", err)}
	}

	object := organizationObject{}
	if err := store.fetchOne(organizations, bson.M{"_id": org.OrgID}, nil, &object); err != nil {
		return timestamp, err
	}

	return object.LastUpdate, nil
}

// RetrieveOrganizationInfo retrieves organization information
func (store *MongoStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	result := organizationObject{}
	if err := store.fetchOne(organizations, bson.M{"_id": orgID}, nil, &result); err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		}
		return nil, nil
	}
	return &common.StoredOrganization{Org: result.Organization, Timestamp: result.LastUpdate}, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *MongoStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	if err := store.removeAll(organizations, bson.M{"_id": orgID}); err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	return nil
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *MongoStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	result := []organizationObject{}
	query := bson.M{}
	if err := store.fetchAll(organizations, query, nil, &result); err != nil {
		return nil, err
	}
	orgs := make([]common.StoredOrganization, 0)
	for _, org := range result {
		orgs = append(orgs, common.StoredOrganization{Org: org.Organization, Timestamp: org.LastUpdate})
	}
	return orgs, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *MongoStorage) RetrieveUpdatedOrganizations(timevalue time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	//timestamp, err := bson.NewMongoTimestamp(timevalue, 1)
	//timestamp := primitive.Timestamp{T: uint32(timevalue.Unix())}

	result := []organizationObject{}
	if err := store.fetchAll(organizations, bson.M{"last-update": bson.M{"$gte": timevalue}}, nil, &result); err != nil {
		return nil, err
	}
	orgs := make([]common.StoredOrganization, 0)
	for _, org := range result {
		orgs = append(orgs, common.StoredOrganization{Org: org.Organization, Timestamp: org.LastUpdate})
	}
	return orgs, nil
}

// AddUsersToACL adds users to an ACL
func (store *MongoStorage) AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.addUsersToACLHelper(acls, aclType, orgID, key, users)
}

// RemoveUsersFromACL removes users from an ACL
func (store *MongoStorage) RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.removeUsersFromACLHelper(acls, aclType, orgID, key, users)
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *MongoStorage) RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	return store.retrieveACLHelper(acls, aclType, orgID, key, aclUserType)
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *MongoStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return store.retrieveACLsInOrgHelper(acls, aclType, orgID)
}

// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
func (store *MongoStorage) RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	return store.retrieveObjOrDestTypeForGivenACLUserHelper(acls, aclType, orgID, aclUserType, aclUsername, aclRole)
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *MongoStorage) IsPersistent() bool {
	return true
}
