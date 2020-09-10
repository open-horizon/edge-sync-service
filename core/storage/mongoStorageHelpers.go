package storage

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

func (store *MongoStorage) getSession() *mgo.Session {
	if store.cacheSize < 2 {
		return store.session
	}
	store.lock()
	session := store.sessionCache[store.cacheIndex]
	store.cacheIndex = (store.cacheIndex + 1) % store.cacheSize
	store.unLock()
	return session
}

func (store *MongoStorage) checkObjects() {
	if !store.connected {
		return
	}

	currentTime := time.Now().UTC().Format(time.RFC3339)
	query := bson.M{
		"$and": []bson.M{
			bson.M{"metadata.expiration": bson.M{"$ne": ""}},
			bson.M{"metadata.expiration": bson.M{"$lte": currentTime}},
			bson.M{"$or": []bson.M{
				bson.M{"status": common.NotReadyToSend},
				bson.M{"status": common.ReadyToSend}}}},
	}

	selector := bson.M{"metadata": bson.ElementDocument, "last-update": bson.ElementTimestamp}
	result := []object{}
	if err := store.fetchAll(objects, query, selector, &result); err != nil {
		if err != mgo.ErrNotFound && log.IsLogging(logger.ERROR) {
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

func (store *MongoStorage) deleteObject(orgID string, objectType string, objectID string, timestamp bson.MongoTimestamp) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting object %s\n", id)
	}

	query := bson.M{"_id": id}
	if timestamp != -1 {
		query = bson.M{"_id": id, "last-update": timestamp}
	}
	if err := store.removeAll(objects, query); err != nil {
		if err == mgo.ErrNotFound && timestamp != -1 {
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

func (store *MongoStorage) copyDataToFile(id string, dataReader io.Reader, isFirstChunk bool, isLastChunk bool) (fileHanlde *fileHandle,
	written int64, err common.SyncServiceError) {
	if isFirstChunk {
		store.removeFile(id)
		fileHanlde, err = store.createFile(id)
	} else {
		fileHanlde = store.getFileHandle(id)
		if fileHanlde == nil {
			err = &Error{fmt.Sprintf("Failed to append the data, the file doesn't exist.")}
			return
		}
	}
	if err != nil {
		err = &Error{fmt.Sprintf("Failed to create file to store the data. Error: %s.", err)}
		return
	}
	written, err = io.Copy(fileHanlde.file, dataReader)
	if err != nil {
		err = &Error{fmt.Sprintf("Failed to write the data to the file. Error: %s.", err)}
		return
	}
	if isLastChunk {
		if err = fileHanlde.file.Close(); err != nil {
			err = &Error{fmt.Sprintf("Failed to close the file. Error: %s.", err)}
			return
		}
		store.deleteFileHandle(id)
	}
	return
}

func (store *MongoStorage) storeDataInFile(id string, data []byte) common.SyncServiceError {
	store.removeFile(id)
	fileHanlde, err := store.createFile(id)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to create file to store the data. Error: %s.", err)}
	}
	n, err := fileHanlde.file.Write(data)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to write the data to the file. Error: %s.", err)}
	}
	if n != len(data) {
		return &Error{fmt.Sprintf("Failed to write all the data: wrote %d instead of %d.", n, len(data))}
	}
	if err = fileHanlde.file.Close(); err != nil {
		return &Error{fmt.Sprintf("Failed to close the file. Error: %s.", err)}
	}
	return nil
}

func (store *MongoStorage) retrievePolicies(query interface{}) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	results := []object{}

	selectedFields := bson.M{"metadata.destination-org-id": bson.ElementString,
		"metadata.object-type": bson.ElementString, "metadata.object-id": bson.ElementString,
		"metadata.destination-policy": bson.ElementDocument,
		"destinations":                bson.ElementArray,
	}
	if err := store.fetchAll(objects, query, selectedFields, &results); err != nil {
		switch err {
		case mgo.ErrNotFound:
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
	function := func(collection *mgo.Collection) error {
		_, err := collection.RemoveAll(query)
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
	function := func(collection *mgo.Collection) error {
		return collection.Find(query).Select(selector).All(result)
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
	function := func(collection *mgo.Collection) error {
		return collection.Find(query).Select(selector).One(result)
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

func (store *MongoStorage) update(collectionName string, selector interface{}, update interface{}) common.SyncServiceError {
	function := func(collection *mgo.Collection) error {
		return collection.Update(selector, update)
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.update(collectionName, selector, update)
	}
	return nil
}

func (store *MongoStorage) upsert(collectionName string, selector interface{}, update interface{}) common.SyncServiceError {
	function := func(collection *mgo.Collection) error {
		_, err := collection.Upsert(selector, update)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, false)
	if err != nil {
		return err
	}

	if retry {
		return store.upsert(collectionName, selector, update)
	}
	return nil
}

func (store *MongoStorage) insert(collectionName string, doc interface{}) common.SyncServiceError {
	function := func(collection *mgo.Collection) error {
		return collection.Insert(doc)
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

func (store *MongoStorage) count(collectionName string, selector interface{}) (uint32, common.SyncServiceError) {
	var count uint32
	function := func(collection *mgo.Collection) error {
		var err error
		countInt, err := collection.Find(selector).Count()
		count = uint32(countInt)
		return err
	}

	retry, err := store.withCollectionHelper(collectionName, function, true)
	if err != nil {
		return 0, err
	}

	if retry {
		return store.count(collectionName, selector)
	}
	return count, nil
}

func (store *MongoStorage) removeFile(id string) common.SyncServiceError {
	function := func(db *mgo.Database) error {
		return db.GridFS("fs").Remove(id)
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

func (store *MongoStorage) openFile(id string) (*fileHandle, common.SyncServiceError) {
	function := func(db *mgo.Database) (*mgo.GridFile, error) {
		return db.GridFS("fs").Open(id)
	}

	file, session, retry, err := store.withDBAndReturnHelper(function, true)
	if err != nil {
		return nil, err
	}

	if retry {
		return store.openFile(id)
	}

	return &fileHandle{file, session, 0, nil}, nil
}

func (store *MongoStorage) createFile(id string) (*fileHandle, common.SyncServiceError) {
	function := func(db *mgo.Database) (*mgo.GridFile, error) {
		return db.GridFS("fs").Create(id)
	}

	file, session, retry, err := store.withDBAndReturnHelper(function, false)
	if err != nil {
		return nil, err
	}

	if retry {
		return store.createFile(id)
	}
	file.SetChunkSize(common.Configuration.MaxDataChunkSize)
	return &fileHandle{file, session, 0, nil}, nil
}

func (store *MongoStorage) run(cmd interface{}, result interface{}) common.SyncServiceError {
	function := func(db *mgo.Database) error {
		return db.Run(cmd, result)
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

func (store *MongoStorage) withDBHelper(function func(*mgo.Database) error, isRead bool) (bool, common.SyncServiceError) {
	if !store.connected {
		return false, &NotConnected{"Disconnected from the database"}
	}

	session := store.getSession()
	db := session.DB(common.Configuration.MongoDbName)

	err := function(db)

	if err == nil || err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
		return false, err
	}
	pingErr := session.Ping()
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return false, err
	}
	session.Refresh()
	pingErr = session.Ping()
	if pingErr == nil {
		db := session.DB(common.Configuration.MongoDbName)
		err := function(db)
		if err == nil || err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
			return false, err
		}
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return false, err
	}

	if connected := store.reconnect(true); connected {
		return true, nil
	}
	return false, &NotConnected{"Disconnected from the database"}
}

func (store *MongoStorage) withDBAndReturnHelper(function func(*mgo.Database) (*mgo.GridFile, error), isRead bool) (*mgo.GridFile,
	*mgo.Session, bool, common.SyncServiceError) {
	if !store.connected {
		return nil, nil, false, &NotConnected{"Disconnected from the database"}
	}
	session := store.getSession()
	db := session.DB(common.Configuration.MongoDbName)

	file, err := function(db)
	if err == nil {
		return file, session, false, nil
	}
	if err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
		return nil, nil, false, err
	}
	pingErr := session.Ping()
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return nil, nil, false, err
	}
	session.Refresh()
	pingErr = session.Ping()
	if pingErr == nil {
		db := session.DB(common.Configuration.MongoDbName)
		file, err := function(db)
		if err == nil {
			return file, session, false, nil
		}
		if err != mgo.ErrNotFound && err != mgo.ErrCursor || mgo.IsDup(err) {
			if isRead {
				common.HealthStatus.DBReadFailed()
			} else {
				common.HealthStatus.DBWriteFailed()
			}
		}
		return nil, nil, false, err
	}

	if connected := store.reconnect(true); connected {
		return nil, nil, true, nil
	}
	return nil, nil, false, &NotConnected{"Disconnected from the database"}
}

func (store *MongoStorage) withCollectionHelper(collectionName string, function func(*mgo.Collection) error, isRead bool) (bool,
	common.SyncServiceError) {
	if !store.connected {
		return false, &NotConnected{"Disconnected from the database"}
	}

	session := store.getSession()
	collection := session.DB(common.Configuration.MongoDbName).C(collectionName)

	err := function(collection)

	if err == nil || err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
		return false, err
	}
	pingErr := session.Ping()
	if pingErr == nil {
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
		}
		return false, err
	}
	session.Refresh()
	pingErr = session.Ping()
	if pingErr == nil {
		collection := session.DB(common.Configuration.MongoDbName).C(collectionName)
		err := function(collection)
		if err == nil || err == mgo.ErrNotFound || err == mgo.ErrCursor || mgo.IsDup(err) {
			return false, err
		}
		if isRead {
			common.HealthStatus.DBReadFailed()
		} else {
			common.HealthStatus.DBWriteFailed()
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

	pingErr := store.session.Ping()
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

	var session *mgo.Session
	var dialErr error
	for i := 0; i < 3; {
		session, dialErr = mgo.DialWithInfo(store.dialInfo)
		if dialErr == nil && session != nil {
			break
		}
		if timeout {
			i++
		}
	}

	if dialErr != nil || session == nil {
		go store.reconnect(false)
		return false
	}

	session.SetSafe(&mgo.Safe{})
	store.session = session
	store.connected = true
	if store.cacheSize > 1 {
		for i := 0; i < store.cacheSize; i++ {
			store.sessionCache[i].Close()
			store.sessionCache[i] = store.session.Copy()
		}
	}

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

func (store *MongoStorage) getFileHandle(id string) (fH *fileHandle) {
	<-store.mapLock
	fH = store.openFiles[id]
	store.mapLock <- 1
	return
}

func (store *MongoStorage) putFileHandle(id string, fH *fileHandle) {
	<-store.mapLock
	store.openFiles[id] = fH
	store.mapLock <- 1
}

func (store *MongoStorage) deleteFileHandle(id string) {
	<-store.mapLock
	delete(store.openFiles, id)
	store.mapLock <- 1
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
			if err == mgo.ErrNotFound {
				result.Users = make([]common.ACLentry, 0)
				result.Users = append(result.Users, users...)
				result.ID = id
				result.OrgID = orgID
				result.ACLType = aclType
				if err = store.insert(collection, result); err != nil {
					if mgo.IsDup(err) {
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
				"$currentDate": bson.M{"last-update": bson.M{"$type": "timestamp"}},
			}); err != nil {
			if err == mgo.ErrNotFound {
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
							if err == mgo.ErrNotFound {
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
					"$currentDate": bson.M{"last-update": bson.M{"$type": "timestamp"}},
				}); err != nil {
				if err == mgo.ErrNotFound {
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
		if err == mgo.ErrNotFound {
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
	selector := bson.M{"_id": bson.ElementString}
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
				"username":    aclUsername,
			},
		}
	} else {
		subquery = bson.M{
			"$elemMatch": bson.M{
				"aclusertype": aclUserType,
				"username":    aclUsername,
				"aclrole":     aclRole,
			},
		}

	}

	query := bson.M{
		"org-id":   orgID,
		"acl-type": aclType,
		"users":    subquery,
	}

	selector := bson.M{"_id": bson.ElementString}
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
