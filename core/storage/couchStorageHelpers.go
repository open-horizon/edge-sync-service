package storage

import (
	"bytes"
	"context"
	"fmt"
	_ "github.com/go-kivik/couchdb/v3" // The CouchDB Driver
	kivik "github.com/go-kivik/kivik/v3"
	"github.com/open-horizon/edge-sync-service/common"
	"io/ioutil"
	"strings"
	"time"
)

//Add better error-handling for functions (meaningful messages)

func (store *CouchStorage) getOne(id string, result interface{}) common.SyncServiceError {

	if exists, err := store.client.DBExists(context.TODO(), store.loginInfo["dbName"]); err != nil || !exists {
		return err
	}

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	row := db.Get(context.TODO(), id)
	if err := row.ScanDoc(&result); err != nil {
		return err
	}

	return nil
}

func (store *CouchStorage) addAttachment(id string, data []byte) common.SyncServiceError {

	if exists, err := store.client.DBExists(context.TODO(), store.loginInfo["dbName"]); err != nil || !exists {
		return err
	}

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	var rev string
	row := db.Get(context.TODO(), id)
	if row == nil {
		return &Error{fmt.Sprintf("Object not found")}
	}

	rev = row.Rev
	content := ioutil.NopCloser(bytes.NewReader(data))
	attachment := &kivik.Attachment{Filename: id, ContentType: "application/octet-stream", Content: content}

	if _, err := db.PutAttachment(context.TODO(), id, rev, attachment); err != nil {
		return err
	}

	defer content.Close()
	return nil
}

func (store *CouchStorage) removeAttachment(id string) common.SyncServiceError {

	if exists, err := store.client.DBExists(context.TODO(), store.loginInfo["dbName"]); err != nil || !exists {
		return err
	}

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	var rev string
	row := db.Get(context.TODO(), id)
	if row == nil {
		return &Error{fmt.Sprintf("Object not found")}
	}

	rev = row.Rev
	if _, err := db.DeleteAttachment(context.TODO(), id, rev, id); err != nil {
		return err
	}

	return nil
}

func (store *CouchStorage) upsert(object couchObject, exists bool) common.SyncServiceError {

	if exists, err := store.client.DBExists(context.TODO(), store.loginInfo["dbName"]); err != nil || !exists {
		return err
	}

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	if !exists {
		if _, err := db.Put(context.TODO(), object.ID, object); err != nil {
			return err
		}
	} else {
		row := db.Get(context.TODO(), object.ID)
		if row == nil {
			return &Error{fmt.Sprintf("Object not found")}
		}
		object.Rev = row.Rev
		if _, err := db.Put(context.TODO(), object.ID, object); err != nil {
			return err
		}
	}

	return nil
}

func (store *CouchStorage) getInstanceID() int64 {
	currentTime, err := store.RetrieveTimeOnServer()
	if err != nil {
		currentTime = time.Now()
	}
	return currentTime.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

//This function should be moved to storage.go after it is finalized
func createDSN(ipAddress, username, password string) string {
	var strBuilder strings.Builder
	strBuilder.WriteString("http://")
	strBuilder.WriteString(username)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(password)
	strBuilder.WriteByte('@')
	strBuilder.WriteString(ipAddress)
	strBuilder.WriteByte('/')
	return strBuilder.String()
}

// func (store *CouchStorage) fetchOne(query interface{}, result interface{}) common.SyncServiceError {

// 	dbname := common.Configuration.CouchDbName
// 	if exists, err := store.client.DBExists(context.TODO(), dbname); err != nil || !exists {
// 		return err
// 	}

// 	db := store.client.DB(context.TODO(), dbname)
// 	var found bool
// 	var rows *kivik.Rows
// 	rows, err := db.Find(context.TODO(), query)
// 	if err != nil {
// 		return err
// 	}
// 	for rows.Next() {
// 		found = true
// 		err := rows.ScanDoc(&result)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	if !found {
// 		return &Error{fmt.Sprintf("Not found")}
// 	}

// 	return nil
// }

// func (store *CouchStorage) deleteObject(id string) common.SyncServiceError {

// 	dbname := common.Configuration.CouchDbName
// 	if dbexists, err := store.client.DBExists(context.TODO(), dbname); err != nil || !dbexists {
// 		return err
// 	}

// 	db := store.client.DB(context.TODO(), dbname)
// 	row := db.Get(context.TODO(), id)
// 	if row == nil {
// 		return nil
// 	}
// 	rev := row.Rev

// 	if _, err := db.Delete(context.TODO(), id, rev); err != nil {
// 		return err
// 	}

// 	return nil
// }
