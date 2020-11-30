package storage

import (
	"bytes"
	"context"
	_ "github.com/go-kivik/couchdb/v3" // The CouchDB Driver
	kivik "github.com/go-kivik/kivik/v3"
	"github.com/open-horizon/edge-sync-service/common"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

func (store *CouchStorage) getOne(id string, result interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	row := db.Get(context.TODO(), id)
	if kivik.StatusCode(row.Err) == http.StatusNotFound {
		return notFound
	}
	if err := row.ScanDoc(&result); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) addAttachment(id string, data []byte) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if kivik.StatusCode(row.Err) == http.StatusNotFound {
		return notFound
	}

	content := ioutil.NopCloser(bytes.NewReader(data))
	defer content.Close()

	attachment := &kivik.Attachment{Filename: id, ContentType: "application/octet-stream", Content: content}
	if _, err := db.PutAttachment(context.TODO(), id, row.Rev, attachment); err != nil {
		return err
	}
	return nil
}

func (store *CouchStorage) removeAttachment(id string) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if kivik.StatusCode(row.Err) == http.StatusNotFound {
		return notFound
	}

	if _, err := db.DeleteAttachment(context.TODO(), id, row.Rev, id); err != nil {
		return err
	}
	return nil
}

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

func (store *CouchStorage) findAll(query interface{}, result interface{}) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])

	rows, err := db.Find(context.TODO(), query)
	if err != nil {
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

func (store *CouchStorage) deleteObject(id string) common.SyncServiceError {

	db := store.client.DB(context.TODO(), store.loginInfo["dbName"])
	row := db.Get(context.TODO(), id)
	if kivik.StatusCode(row.Err) == http.StatusNotFound {
		return notFound
	}

	if _, err := db.Delete(context.TODO(), id, row.Rev); err != nil {
		return err
	}
	return nil
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
