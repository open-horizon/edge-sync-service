package storage

import (
	"context"
	"fmt"
	_ "github.com/go-kivik/couchdb/v3" // The CouchDB Driver
	kivik "github.com/go-kivik/kivik/v3"
	"github.com/open-horizon/edge-sync-service/common"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

// Note:
// To update/delete an object in CouchDB, the first step is always to GET() the object
// This is because the current 'rev' i.e 'revision' is needed to perform any modifications
// Due to this - there are no direct methods to delete/update objects based on queries

func (store *CouchStorage) checkObjects() {
	if !store.connected {
		return
	}

	var err error
	currentTime := time.Now().UTC().Format(time.RFC3339)
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"$and": []map[string]interface{}{
				map[string]interface{}{"metadata.expiration": map[string]interface{}{"$ne": ""}},
				map[string]interface{}{"metadata.expiration": map[string]interface{}{"$lte": currentTime}},
				map[string]interface{}{"$or": []map[string]interface{}{
					map[string]interface{}{"status": common.NotReadyToSend},
					map[string]interface{}{"status": common.ReadyToSend}}}},
		},
		"fields": []string{"metadata", "last-update"},
	}

	result := []couchObject{}
	if err := store.findAll(query, &result); err != nil {
		if err != notFound && log.IsLogging(logger.ERROR) {
			log.Error("Error in CouchStorage.checkObjects: failed to remove expired objects. Error: %s\n", err)
		}
		return
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Removing expired objects")
	}

	for _, object := range result {

		id := createObjectCollectionID(object.MetaData.DestOrgID, object.MetaData.ObjectType, object.MetaData.ObjectID)
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Deleting object %s\n", id)
		}

		var query interface{}
		query = map[string]interface{}{"selector": map[string]interface{}{"_id": id}}
		if !object.LastUpdate.IsZero() {
			query = map[string]interface{}{"selector": map[string]interface{}{"_id": id, "last-update": object.LastUpdate}}
		}

		if err = store.deleteAllCouchObjects(query); err != nil {
			if err != notFound || object.LastUpdate.IsZero() {
				log.Error("Error in CouchStorage.checkObjects: failed to remove expired objects. Error: %s\n", err)
			}
		}

		if err == nil {
			store.DeleteNotificationRecords(object.MetaData.DestOrgID, object.MetaData.ObjectType, object.MetaData.ObjectID, "", "")
		} else if log.IsLogging(logger.ERROR) {
			log.Error("Error in CouchStorage.checkObjects: failed to remove expired objects. Error: %s\n", err)
		}
	}
}

func (store *CouchStorage) getOne(id string, result interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	row := db.Get(context.TODO(), id)
	// Other Runtime errors are returned after the row.ScanDoc() call
	if kivik.StatusCode(row.Err) == http.StatusNotFound {
		return notFound
	}
	if err := row.ScanDoc(&result); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) addAttachment(id string, dataReader io.Reader) (int64, common.SyncServiceError) {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if row.Err != nil {
		if kivik.StatusCode(row.Err) == http.StatusNotFound {
			return 0, notFound
		}
		return 0, row.Err
	}

	content := ioutil.NopCloser(dataReader)
	defer content.Close()

	attachment := &kivik.Attachment{Filename: id, ContentType: "application/octet-stream", Content: content}
	_, err := db.PutAttachment(context.TODO(), id, row.Rev, attachment)
	if err != nil {
		return 0, err
	}

	attachmentMeta, err := db.GetAttachmentMeta(context.TODO(), id, id)
	if err != nil {
		return 0, err
	}

	return attachmentMeta.Size, nil
}

func (store *CouchStorage) removeAttachment(id string) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if row.Err != nil {
		if kivik.StatusCode(row.Err) == http.StatusNotFound {
			return notFound
		}
		return row.Err
	}
	if _, err := db.DeleteAttachment(context.TODO(), id, row.Rev, id); err != nil {
		return err
	}
	return nil
}

// In case of updating existing object, Rev is needs to be set in object
// This has been set in the passed object by calling function
func (store *CouchStorage) upsertObject(id string, object interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	if _, err := db.Put(context.TODO(), id, object); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) getInstanceID() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

// Storage interface has many types of objects - couchObject, couchDestinationObject, couchACLObject, etc.
// To make this a generic function for all object types, address of relevant type's slice is passed in result interface
// Reflection is needed to be able to access and modify the underlying slice i.e
// append each object to it after iterating over the rows returned
func (store *CouchStorage) findAll(query interface{}, result interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return notFound
		}
		return err
	}

	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr {
		return &Error{"result argument must be a slice address"}
	}

	slicev := resultv.Elem()

	if slicev.Kind() == reflect.Interface {
		slicev = slicev.Elem()
	}
	if slicev.Kind() != reflect.Slice {
		return &Error{"result argument must be a slice address"}
	}

	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0
	for rows.Next() {
		elemp := reflect.New(elemt)
		err := rows.ScanDoc(elemp.Interface())
		if err != nil {
			return err
		}

		slicev = reflect.Append(slicev, elemp.Elem())
		slicev = slicev.Slice(0, slicev.Cap())
		i++
	}
	resultv.Elem().Set(slicev.Slice(0, i))

	if err := rows.Err(); err != nil {
		return err
	}
	return rows.Close()
}

func (store *CouchStorage) countObjects(query interface{}) (uint32, common.SyncServiceError) {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return 0, nil
		}
		return 0, err
	}

	var count uint32
	for rows.Next() {
		count++
	}

	if err := rows.Err(); err != nil {
		return 0, err
	}

	return count, nil
}

func (store *CouchStorage) deleteObject(id string) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if row.Err != nil {
		if kivik.StatusCode(row.Err) == http.StatusNotFound {
			return notFound
		}
		return row.Err
	}

	if _, err := db.Delete(context.TODO(), id, row.Rev); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) deleteAllCouchObjects(query interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return notFound
		}
		return err
	}

	for rows.Next() {

		var tempObject couchObject
		err = rows.ScanDoc(&tempObject)
		if err != nil {
			return err
		}

		if _, err := db.Delete(context.TODO(), tempObject.ID, tempObject.Rev); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) deleteAllDestinationObjects(query interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return notFound
		}
		return err
	}

	for rows.Next() {

		var tempObject couchDestinationObject
		err = rows.ScanDoc(&tempObject)
		if err != nil {
			return err
		}

		if _, err := db.Delete(context.TODO(), tempObject.ID, tempObject.Rev); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) deleteAllNotificationObjects(query interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return notFound
		}
		return err
	}

	for rows.Next() {

		var tempObject couchNotificationObject
		err = rows.ScanDoc(&tempObject)
		if err != nil {
			return err
		}

		if _, err := db.Delete(context.TODO(), tempObject.ID, tempObject.Rev); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) deleteAllACLObjects(query interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return notFound
		}
		return err
	}

	for rows.Next() {

		var tempObject couchACLObject
		err = rows.ScanDoc(&tempObject)
		if err != nil {
			return err
		}

		if _, err := db.Delete(context.TODO(), tempObject.ID, tempObject.Rev); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) getAttachment(id string) (*kivik.Attachment, common.SyncServiceError) {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	attachment, err := db.GetAttachment(context.TODO(), id, id)
	if err != nil {
		if kivik.StatusCode(err) == http.StatusNotFound {
			return nil, notFound
		}
		return nil, err
	}
	return attachment, nil
}

func (store *CouchStorage) retrievePolicies(query interface{}) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	results := []couchObject{}

	if err := store.findAll(query, &results); err != nil {
		if err == notFound {
			return nil, nil
		}
		return nil, &Error{fmt.Sprintf("Failed to fetch the objects with a Destination Policy. Error: %s", err)}
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

func (store *CouchStorage) addUsersToACLHelper(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Adding a %s ACL for %s\n", aclType, id)
	}
	result := &couchACLObject{}
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
			result.Users = make([]common.ACLentry, 0)
			result.Users = append(result.Users, users...)
			result.ID = id
			result.OrgID = orgID
			result.ACLType = aclType
			if err = store.upsertObject(id, result); err != nil {
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

	result.Users = integratedUsernames
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to add a %s ACL. Error: %s.", aclType, err)}
	}

	return nil
}

func (store *CouchStorage) removeUsersFromACLHelper(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting a %s ACL for %s\n", aclType, id)
	}
	result := &couchACLObject{}
	if err := store.getOne(id, &result); err != nil {
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
					if err := store.deleteObject(id); err != nil {
						if err == notFound {
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
		result.LastUpdate = time.Now()
		if err := store.upsertObject(id, result); err != nil {
			return &Error{fmt.Sprintf("Failed to delete a %s ACL. Error: %s.", aclType, err)}
		}
	}
	return nil
}

func (store *CouchStorage) retrieveACLHelper(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	var id string
	if key == "" {
		id = orgID + ":" + aclType + ":*"
	} else {
		id = orgID + ":" + aclType + ":" + key
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving a %s ACL for %s, aclUserType %s\n", aclType, id, aclUserType)
	}
	result := &couchACLObject{}
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
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

func (store *CouchStorage) retrieveACLsInOrgHelper(aclType string, orgID string) ([]string, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving the %s ACL types for %s\n", aclType, orgID)
	}

	var docs []couchACLObject
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"org-id":   orgID,
			"acl-type": aclType},
		"fields": []string{"_id"},
	}
	if err := store.findAll(query, &docs); err != nil {
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

func (store *CouchStorage) retrieveObjOrDestTypeForGivenACLUserHelper(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving %s types for ACL user %s:%s\n", aclType, aclUserType, aclUsername)
	}

	docs := []couchACLObject{}
	var subquery map[string]interface{}
	if aclRole == "" || aclRole == "*" {
		subquery = map[string]interface{}{
			"$elemMatch": map[string]interface{}{
				"aclusertype": aclUserType,
				"username":    aclUsername,
			},
		}
	} else {
		subquery = map[string]interface{}{
			"$elemMatch": map[string]interface{}{
				"aclusertype": aclUserType,
				"username":    aclUsername,
				"aclrole":     aclRole,
			},
		}

	}

	query := map[string]interface{}{
		"org-id":   orgID,
		"acl-type": aclType,
		"users":    subquery,
	}

	finalQuery := map[string]interface{}{
		"selector": query,
		"fields":   []string{"_id"},
	}

	if err := store.findAll(finalQuery, &docs); err != nil {
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

func createDSN(ipAddress string) string {
	var strBuilder strings.Builder
	if common.Configuration.CouchUseSSL {
		strBuilder.WriteString("https://")
	} else {
		strBuilder.WriteString("http://")
	}
	strBuilder.WriteString(ipAddress)
	if common.Configuration.CouchUseSSL {
		strBuilder.WriteString(":6984/")
	} else {
		strBuilder.WriteString(":5984/")
	}
	return strBuilder.String()
}
