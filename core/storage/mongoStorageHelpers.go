package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func (store *MongoStorage) getMongoClient() *mongo.Client {
	// Need lock??
	store.lock()
	client := store.client
	store.unLock()
	return client
}

func (store *MongoStorage) checkObjects() {
	if !store.connected {
		return
	}

	currentTime := time.Now().UTC().Format(time.RFC3339)
	query := bson.M{
		"$and": bson.A{
			bson.M{"metadata.expiration": bson.M{"$ne": ""}},
			bson.M{"metadata.expiration": bson.M{"$lte": currentTime}},
			bson.M{"$or": bson.A{
				bson.M{"status": common.NotReadyToSend},
				bson.M{"status": common.ReadyToSend},
				bson.M{"status": common.Verifying},
				bson.M{"status": common.VerificationFailed}}}},
	}

	selector := bson.M{"metadata": 1, "last-update": 1}
	result := []object{}
	if err := store.fetchAll(objects, query, selector, &result); err != nil {
		if err != mongo.ErrNoDocuments && log.IsLogging(logger.ERROR) {
			log.Error("Error in mongoStorage.checkObjects: failed to remove expired objects. Error: %s\n", err)
		}
		return
	}
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Removing expired objects")
	}

	for _, object := range result {
		err := store.deleteObject(object.MetaData.DestOrgID, object.MetaData.ObjectType, object.MetaData.ObjectID, object.LastUpdate)
		if err == nil {
			store.DeleteNotificationRecords(object.MetaData.DestOrgID, object.MetaData.ObjectType, object.MetaData.ObjectID, "", "")
		} else if log.IsLogging(logger.ERROR) {
			log.Error("Error in mongoStorage.checkObjects: failed to remove expired objects. Error: %s\n", err)
		}
	}
}

func (store *MongoStorage) deleteObject(orgID string, objectType string, objectID string, timestamp time.Time) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting object %s\n", id)
	}

	query := bson.M{"_id": id}
	if !timestamp.IsZero() {
		query = bson.M{"_id": id, "last-update": timestamp}
	}

	if err := store.removeAll(objects, query); err != nil {
		if err == mongo.ErrNoDocuments && !timestamp.IsZero() {
			return nil
		}
		return &Error{fmt.Sprintf("Failed to delete object. Error: %s.", err)}
	}

	if err := store.removeFile(id); err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Error in deleteStoredObject: failed to delete data file. Error: %s\n", err)
		}
	}
	return nil
}

// append data stream to mongodb data
func (store *MongoStorage) copyDataToFile(id string, dataReader io.Reader) (err common.SyncServiceError) {
	store.removeFile(id)
	err = store.createFile(id, dataReader)
	if err != nil {
		err = &Error{fmt.Sprintf("Failed to create file to store the data. Error: %s.", err)}
		return
	}

	return
}

// stores the data bytes into mongodb
func (store *MongoStorage) storeDataInFile(id string, data []byte) common.SyncServiceError {
	store.removeFile(id)
	br := bytes.NewReader(data)
	if err := store.createFile(id, br); err != nil {
		return &Error{fmt.Sprintf("Failed to create file to store the data. Error: %s.", err)}
	}
	return nil
}

func (store *MongoStorage) retrievePolicies(query interface{}) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	results := []object{}

	selectedFields := bson.M{"metadata.destination-org-id": 1,
		"metadata.object-type": 1, "metadata.object-id": 1,
		"metadata.destination-policy": 1,
		"destinations":                1,
	}
	if err := store.fetchAll(objects, query, selectedFields, &results); err != nil {
		switch err {
		case mongo.ErrNoDocuments:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the objects with a Destination Policy. Error: %s", err)}
		}
	}

	objects := make([]common.ObjectDestinationPolicy, len(results))
	for index, oneResult := range results {
		destinationList := make([]common.DestinationsStatus, len(oneResult.Destinations))
		for destIndex, destination := range oneResult.Destinations {
			destinationList[destIndex] = common.DestinationsStatus{
				DestType: destination.Destination.DestType, DestID: destination.Destination.DestID,
				Status: destination.Status, Message: destination.Message,
			}
		}
		objects[index] = common.ObjectDestinationPolicy{
			OrgID: oneResult.MetaData.DestOrgID, ObjectType: oneResult.MetaData.ObjectType, ObjectID: oneResult.MetaData.ObjectID,
			DestinationPolicy: oneResult.MetaData.DestinationPolicy, Destinations: destinationList,
		}
	}
	return objects, nil
}

func (store *MongoStorage) removeAll(collectionName string, query interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		_, err := collection.DeleteMany(context.TODO(), query)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.removeAll(collectionName, query)
	}
	return nil
}

func (store *MongoStorage) fetchAll(collectionName string, query interface{}, selector interface{}, result interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		opts := options.Find().SetProjection(selector)
		// selector looks like: bson.D{{"field1", 1}, {"field2", 1}}, 1 means include

		cursor, err := collection.Find(context.TODO(), query, opts)
		if err != nil {
			return err
		}

		return cursor.All(context.TODO(), result)
	}

	retry, err := store.withCollectionHelper(collectionName, function, true)
	if err != nil {
		return err
	}

	if retry {
		return store.fetchAll(collectionName, query, selector, result)
	}
	return nil
}

func (store *MongoStorage) fetchOne(collectionName string, query interface{}, selector interface{}, result interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		opts := options.FindOne()
		return collection.FindOne(context.TODO(), query, opts).Decode(result)
	}

	retry, err := store.withCollectionHelper(collectionName, function, true)
	if err != nil {
		return err
	}

	if retry {
		return store.fetchOne(collectionName, query, selector, result)
	}
	return nil
}

func (store *MongoStorage) update(collectionName string, filter interface{}, update interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		opts := options.Update()
		updatedResult, err := collection.UpdateOne(context.TODO(), filter, update, opts)
		v := int64(0)
		if updatedResult.MatchedCount == v {
			return &NotFound{}
		}

		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.update(collectionName, filter, update)
	}
	return nil
}

func (store *MongoStorage) upsert(collectionName string, filter interface{}, update interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		opts := options.Update().SetUpsert(true)
		_, err := collection.UpdateOne(context.TODO(), filter, update, opts)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.upsert(collectionName, filter, update)
	}
	return nil
}

func (store *MongoStorage) insert(collectionName string, doc interface{}) common.SyncServiceError {
	function := func(collection *mongo.Collection) error {
		_, err := collection.InsertOne(context.TODO(), doc)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.insert(collectionName, doc)
	}
	return nil
}

func (store *MongoStorage) count(collectionName string, filter interface{}) (uint32, common.SyncServiceError) {
	var count uint32
	function := func(collection *mongo.Collection) error {
		countInt, err := collection.CountDocuments(context.TODO(), filter)
		count = uint32(countInt)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, true)
	if err != nil {
		return 0, err
	}

	if retry {
		return store.count(collectionName, filter)
	}
	return count, nil
}

func (store *MongoStorage) getFileInfo(id string) (*gridfsFile, common.SyncServiceError) {
	filter := bson.D{{"filename", id}}

	if store.gridfsBucket == nil {
		gridfsBucket, err := gridfs.NewBucket(store.database)
		if err != nil {
			return nil, err
		}
		store.gridfsBucket = gridfsBucket
	}
	cursor, err := store.gridfsBucket.Find(filter)
	if err != nil {
		return nil, err
	}
	var foundFiles []gridfsFile
	if err = cursor.All(context.TODO(), &foundFiles); err != nil {
		return nil, err
	} else if len(foundFiles) == 0 {
		return nil, &NotFound{fmt.Sprintf("File %v not found in mongo db", id)}
	}

	return &foundFiles[0], nil
}

func (store *MongoStorage) removeFile(id string) common.SyncServiceError {
	function := func(db *mongo.Database) error {
		file, err := store.getFileInfo(id)
		if err != nil {
			return err
		}

		gridfsBucket, err := gridfs.NewBucket(db)
		if err != nil {
			return err
		}
		dbId, err := primitive.ObjectIDFromHex(file.Id)
		if err != nil {
			return err
		}
		return gridfsBucket.Delete(dbId)
	}

	retry, err := store.withDBHelper(function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.removeFile(id)
	}

	return nil
}

func (store *MongoStorage) openFile(id string) (*gridfs.DownloadStream, common.SyncServiceError) {
	function := func(db *mongo.Database) (*gridfs.DownloadStream, error) {
		return store.gridfsBucket.OpenDownloadStreamByName(id)
	}

	downloadStream, retry, err := store.withDBAndReturnHelper(function, true)
	if err != nil {
		return nil, err
	}

	if retry {
		return store.openFile(id)
	}

	return downloadStream, nil
}

// Save file into mongo gridFS
func (store *MongoStorage) createFile(id string, data io.Reader) common.SyncServiceError {

	function := func(db *mongo.Database) error {
		var err error
		bucket := store.gridfsBucket
		if bucket == nil {
			if bucket, err = gridfs.NewBucket(db); err != nil {
				return err
			}
		}

		uploadOpts := options.GridFSUpload().SetChunkSizeBytes(int32(common.Configuration.MaxDataChunkSize))
		// filename of the object in fs.File is the value of id
		if uploadStream, err := bucket.OpenUploadStream(id, uploadOpts); err != nil {
			return err
		} else {
			_, err = io.Copy(uploadStream, data)
			uploadStream.Close()
			return err
		}
	}

	retry, err := store.withDBHelper(function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.createFile(id, data)
	}

	return nil
}

func (store *MongoStorage) run(cmd interface{}, result interface{}) common.SyncServiceError {
	function := func(db *mongo.Database) error {
		return db.RunCommand(context.TODO(), cmd).Decode(result)
	}

	retry, err := store.withDBHelper(function, true)
	if err != nil {
		return err
	}

	if retry {
		return store.run(cmd, result)
	}
	return nil
}

func (store *MongoStorage) withDBHelper(function func(*mongo.Database) error, isRead bool) (bool, common.SyncServiceError) {
	if !store.connected {
		return false, &NotConnected{"Disconnected from the database"}
	}

	mongoClient := store.getMongoClient()
	db := mongoClient.Database(common.Configuration.MongoDbName)

	err := function(db)
	if err == nil || err == mongo.ErrNoDocuments || err == mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) || IsNotFound(err) {
		return false, err
	}

	pingErr := mongoClient.Ping(context.Background(), nil)
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return false, pingErr
	}

	// reach here if has ping err
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20*time.Second))
	defer cancel()
	mongoClient, err = mongo.Connect(ctx, store.clientConnectOpts)
	if err != nil {
		return false, err
	}
	pingErr = mongoClient.Ping(context.Background(), nil)
	if pingErr == nil {
		db := mongoClient.Database(common.Configuration.MongoDbName)
		store.database = db
		gridfsBucket, err := gridfs.NewBucket(db)
		if err != nil {
			return false, nil
		}
		store.gridfsBucket = gridfsBucket
		err = function(db)
		if err == nil || err == mongo.ErrNoDocuments || err == mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) || IsNotFound(err) {
			if isRead {
				common.HealthStatus.DBReadFailed()
			} else {
				common.HealthStatus.DBWriteFailed()
			}
		}
		return false, err
	}

	if connected := store.reconnect(true); connected {
		return true, nil
	}
	return false, &NotConnected{"Disconnected from the database"}
}

func (store *MongoStorage) withDBAndReturnHelper(function func(*mongo.Database) (*gridfs.DownloadStream, error), isRead bool) (*gridfs.DownloadStream, bool, common.SyncServiceError) {
	if !store.connected {
		return nil, false, &NotConnected{"Disconnected from the database"}
	}

	mongoClient := store.getMongoClient()
	db := mongoClient.Database(common.Configuration.MongoDbName)

	fileHandler, err := function(db)
	if err == nil {
		return fileHandler, false, nil
	}
	if err == mongo.ErrNoDocuments || err == mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) || IsNotFound(err) {
		return nil, false, err
	}
	pingErr := mongoClient.Ping(context.Background(), nil)
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return nil, false, pingErr
	}

	// reach here if has ping err
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20*time.Second))
	defer cancel()
	mongoClient, err = mongo.Connect(ctx, store.clientConnectOpts)
	if err != nil {
		return nil, false, err
	}
	pingErr = mongoClient.Ping(context.Background(), nil)
	if pingErr == nil {
		db := mongoClient.Database(common.Configuration.MongoDbName)
		store.database = db
		gridfsBucket, err := gridfs.NewBucket(db)
		if err != nil {
			return nil, false, err
		}
		store.gridfsBucket = gridfsBucket

		fileHandler, err := function(db)
		if err == nil {
			return fileHandler, false, nil
		}
		if err != mongo.ErrNoDocuments && !IsNotFound(err) && err != mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) {
			if isRead {
				common.HealthStatus.DBReadFailed()
			} else {
				common.HealthStatus.DBWriteFailed()
			}
		}
		return nil, false, err
	}

	if connected := store.reconnect(true); connected {
		return nil, true, nil
	}
	return nil, false, &NotConnected{"Disconnected from the database"}
}

func (store *MongoStorage) withCollectionHelper(collectionName string, function func(*mongo.Collection) error, isRead bool) (bool,
	common.SyncServiceError) {
	if !store.connected {
		return false, &NotConnected{"Disconnected from the database"}
	}

	mongoClient := store.getMongoClient()
	collection := mongoClient.Database(common.Configuration.MongoDbName).Collection(collectionName)
	err := function(collection)
	if err == nil || err == mongo.ErrNoDocuments || err == mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) || IsNotFound(err) {
		return false, err
	}

	pingErr := mongoClient.Ping(context.Background(), nil)
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return false, pingErr
	}

	// reach here if has ping err
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20*time.Second))
	defer cancel()
	mongoClient, err = mongo.Connect(ctx, store.clientConnectOpts)
	if err != nil {
		return false, err
	}
	pingErr = mongoClient.Ping(context.Background(), nil)

	if pingErr == nil {
		db := mongoClient.Database(common.Configuration.MongoDbName)
		store.database = db
		gridfsBucket, err := gridfs.NewBucket(db)
		if err != nil {
			return false, nil
		}
		store.gridfsBucket = gridfsBucket
		collection = db.Collection(collectionName)

		err = function(collection)
		if err == nil || err == mongo.ErrNoDocuments || IsNotFound(err) || err == mongo.ErrNilCursor || mongo.IsDuplicateKeyError(err) {
			if isRead {
				common.HealthStatus.DBReadFailed()
			} else {
				common.HealthStatus.DBWriteFailed()
			}
		}
		return false, err
	}

	if connected := store.reconnect(true); connected {
		return true, nil
	}

	return false, &NotConnected{"Disconnected from the database"}
}

func (store *MongoStorage) reconnect(timeout bool) bool {
	common.GoRoutineStarted()
	defer common.GoRoutineEnded()

	if !store.connected && timeout {
		return false
	}

	store.lock()
	defer store.unLock()

	if !store.connected && timeout {
		return false
	}

	c := store.getMongoClient()
	pingErr := c.Ping(context.Background(), nil)
	if pingErr == nil {
		store.connected = true
		return true
	}

	store.connected = false

	common.HealthStatus.DisconnectedFromDatabase()
	if trace.IsLogging(logger.ERROR) {
		trace.Error("Disconnected from the database")
	}
	if log.IsLogging(logger.ERROR) {
		log.Error("Disconnected from the database")
	}

	var mongoClient *mongo.Client
	var dialErr error
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(20*time.Second))
	defer cancel()

	for i := 0; i < 3; {
		mongoClient, dialErr = mongo.Connect(ctx, store.clientConnectOpts)
		if dialErr == nil && mongoClient != nil {
			break
		}
		if timeout {
			i++
		}
	}

	if dialErr != nil || mongoClient == nil {
		go store.reconnect(false)
		return false
	}

	store.client = mongoClient
	store.connected = true

	common.HealthStatus.ReconnectedToDatabase()

	if trace.IsLogging(logger.INFO) {
		trace.Info("Reconnected to the database")
	}
	if log.IsLogging(logger.INFO) {
		log.Info("Reconnected to the database")
	}

	return true
}

func (store *MongoStorage) lock() {
	<-store.lockChannel
}

func (store *MongoStorage) unLock() {
	store.lockChannel <- 1
}

func (store *MongoStorage) addUsersToACLHelper(collection string, aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Adding a %s ACL for %s\n", aclType, id)
	}
	result := &aclObject{}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(collection, bson.M{"_id": id}, nil, &result); err != nil {
			if err == mongo.ErrNoDocuments {
				result.Users = make([]common.ACLentry, 0)
				result.Users = append(result.Users, users...)
				result.ID = id
				result.OrgID = orgID
				result.ACLType = aclType
				if err = store.insert(collection, result); err != nil {
					if mongo.IsDuplicateKeyError(err) {
						continue
					}
					return &Error{fmt.Sprintf("Failed to insert a %s ACL. Error: %s.", aclType, err)}
				}
				return nil
			}
			return &Error{fmt.Sprintf("Failed to add a %s ACL. Error: %s.", aclType, err)}
		}

		integratedUsernames := make([]common.ACLentry, 0)
		integratedUsernames = append(integratedUsernames, users...)

		// If the entry in database is not in the input list, add it to input list
		for _, entry := range result.Users {
			add := true
			entryUserTypeInDB := entry.ACLUserType
			entryUsernameInDB := entry.Username

			for _, user := range users {
				// username from input, only compare the {aclType} and {username} to determin if the entry already exists
				userTypeFromRequest := user.ACLUserType
				userNameFromRequest := user.Username
				if entryUserTypeInDB == userTypeFromRequest && entryUsernameInDB == userNameFromRequest {
					add = false
					break
				}
			}

			if add {
				integratedUsernames = append(integratedUsernames, entry)
			}
		}

		if err := store.update(collection, bson.M{"_id": id, "last-update": result.LastUpdate},
			bson.M{
				"$set":         bson.M{"users": integratedUsernames},
				"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
			}); err != nil {
			if IsNotFound(err) {
				continue
			}
			return &Error{fmt.Sprintf("Failed to add a %s ACL. Error: %s.", aclType, err)}
		}

		return nil
	}
	return &Error{fmt.Sprintf("Failed to add a %s ACL.", aclType)}
}

func (store *MongoStorage) removeUsersFromACLHelper(collection string, aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting a %s ACL for %s\n", aclType, id)
	}
	result := &aclObject{}
	for i := 0; i < maxUpdateTries; i++ {
		if err := store.fetchOne(collection, bson.M{"_id": id}, nil, &result); err != nil {
			return &Error{fmt.Sprintf("Failed to delete a %s ACL. Error: %s.", aclType, err)}
		}
		deleted := false
		for _, user := range users {
			userTypeFromRequest := user.ACLUserType
			userNameFromRequest := user.Username
			for i, entry := range result.Users {
				entryUserTypeInDB := entry.ACLUserType
				entryUsernameInDB := entry.Username
				if entryUserTypeInDB == userTypeFromRequest && entryUsernameInDB == userNameFromRequest {
					if len(result.Users) == 1 {
						// Deleting the last username, delete the ACL
						if err := store.removeAll(collection, bson.M{"_id": id}); err != nil {
							if err == mongo.ErrNoDocuments {
								return nil
							}
							return &Error{fmt.Sprintf("Failed to delete a %s ACL. Error: %s.", aclType, err)}
						}
						return nil
					}

					result.Users[i] = result.Users[len(result.Users)-1]
					result.Users = result.Users[:len(result.Users)-1]
					deleted = true
					break
				}

			}
		}
		if deleted {
			if err := store.update(collection, bson.M{"_id": id, "last-update": result.LastUpdate},
				bson.M{
					"$set":         bson.M{"users": result.Users},
					"$currentDate": bson.M{"last-update": bson.M{"$type": "date"}},
				}); err != nil {
				if IsNotFound(err) {
					continue
				}
				return &Error{fmt.Sprintf("Failed to delete a %s ACL. Error: %s.", aclType, err)}
			}
		}
		return nil
	}
	return &Error{fmt.Sprintf("Failed to delete a %s ACL.", aclType)}
}

func (store *MongoStorage) retrieveACLHelper(collection string, aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving a %s ACL for %s, aclUserType %s\n", aclType, id, aclUserType)
	}
	result := &aclObject{}
	if err := store.fetchOne(collection, bson.M{"_id": id}, nil, &result); err != nil {
		if err == mongo.ErrNoDocuments {
			return make([]common.ACLentry, 0), nil
		}
		return nil, err
	}

	users := make([]common.ACLentry, 0)
	if aclUserType != "" {
		for _, entry := range result.Users {
			if entry.ACLUserType == aclUserType {
				users = append(users, entry)
			}
		}
		return users, nil

	}

	return result.Users, nil
}

func (store *MongoStorage) retrieveACLsInOrgHelper(collection string, aclType string, orgID string) ([]string, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving the %s ACL types for %s\n", aclType, orgID)
	}

	var docs []aclObject
	query := bson.M{"org-id": orgID, "acl-type": aclType}
	selector := bson.M{"_id": 1}
	if err := store.fetchAll(collection, query, selector, &docs); err != nil {
		return nil, err
	}

	result := make([]string, 0)
	for _, doc := range docs {
		if parts := strings.Split(doc.ID, ":"); len(parts) == 3 {
			result = append(result, parts[2])
		}

	}
	return result, nil
}

func (store *MongoStorage) retrieveObjOrDestTypeForGivenACLUserHelper(collection string, aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving %s types for ACL user %s:%s\n", aclType, aclUserType, aclUsername)
	}

	docs := []aclObject{}
	var subquery bson.M
	if aclRole == "" || aclRole == "*" {
		subquery = bson.M{
			"$elemMatch": bson.M{
				"aclusertype": aclUserType,
				"$or": bson.A{
					bson.M{"username": aclUsername},
					bson.M{"username": "*"},
				},
			},
		}
	} else {
		subquery = bson.M{
			"$elemMatch": bson.M{
				"aclusertype": aclUserType,
				"aclrole":     aclRole,
				"$or": bson.A{
					bson.M{"username": aclUsername},
					bson.M{"username": "*"},
				},
			},
		}

	}

	query := bson.M{
		"org-id":   orgID,
		"acl-type": aclType,
		"users":    subquery,
	}

	selector := bson.M{"_id": 1}
	if err := store.fetchAll(collection, query, selector, &docs); err != nil {
		return nil, err
	}

	result := make([]string, 0)
	for _, doc := range docs {
		if parts := strings.Split(doc.ID, ":"); len(parts) == 3 {
			result = append(result, parts[2])
		}

	}
	return result, nil

}

func (store *MongoStorage) getInstanceID() int64 {
	currentTime, err := store.RetrieveTimeOnServer()
	if err != nil {
		currentTime = time.Now()
	}
	return currentTime.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
