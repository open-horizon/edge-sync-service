package storage

import (
	"github.com/go-kivik/couchdb/v3" // The CouchDB Driver
	"github.com/go-kivik/couchdb/v3/chttp"
	kivik "github.com/go-kivik/kivik/v3"
	"strings"

	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

// CouchStorage is a CouchDB based store
type CouchStorage struct {
	client    *kivik.Client
	loginInfo map[string]string
	dsn       string
	connected bool
}

type couchObject struct {
	ID                 string                          `json:"_id"`
	Rev                string                          `json:"_rev,omitempty"`
	MetaData           common.MetaData                 `json:"metadata"`
	Attachments        *kivik.Attachments              `json:"_attachments,omitempty"`
	Status             string                          `json:"status"`
	PolicyReceived     bool                            `json:"policy-received"`
	RemainingConsumers int                             `json:"remaining-consumers"`
	RemainingReceivers int                             `json:"remaining-receivers"`
	Destinations       []common.StoreDestinationStatus `json:"destinations"`
	LastUpdate         time.Time                       `json:"last-update"`
}

type couchDestinationObject struct {
	ID           string             `json:"_id"`
	Rev          string             `json:"_rev,omitempty"`
	Destination  common.Destination `json:"destination"`
	LastPingTime time.Time          `json:"last-ping-time"`
}

type couchWebhookObject struct {
	ID         string    `json:"_id"`
	Rev        string    `json:"_rev,omitempty"`
	Hooks      []string  `json:"hooks"`
	LastUpdate time.Time `json:"last-update"`
}

type couchNotificationObject struct {
	ID           string              `json:"_id"`
	Rev          string              `json:"_rev,omitempty"`
	Notification common.Notification `json:"notification"`
}

type couchMessagingGroupObject struct {
	ID         string    `json:"_id"`
	Rev        string    `json:"_rev,omitempty"`
	GroupName  string    `json:"group-name"`
	LastUpdate time.Time `json:"last-update"`
}

type couchOrganizationObject struct {
	ID           string              `json:"_id"`
	Rev          string              `json:"_rev,omitempty"`
	Organization common.Organization `json:"org"`
	LastUpdate   time.Time           `json:"last-update"`
}

type couchACLObject struct {
	ID         string            `json:"_id"`
	Rev        string            `json:"_rev,omitempty"`
	Users      []common.ACLentry `json:"users"`
	OrgID      string            `json:"org-id"`
	ACLType    string            `json:"acl-type"`
	LastUpdate time.Time         `json:"last-update"`
}

// Init initializes the CouchStorage store
func (store *CouchStorage) Init() common.SyncServiceError {

	store.loginInfo = map[string]string{
		"ipAddress": common.Configuration.CouchAddress,
		"dbName":    common.Configuration.CouchDbName,
		"Username":  common.Configuration.CouchUsername,
		"Password":  common.Configuration.CouchPassword,
	}

	store.dsn = createDSN(store.loginInfo["ipAddress"])
	var client *kivik.Client
	var err error

	client, err = kivik.New("couch", store.dsn)
	if client == nil || err != nil {
		message := fmt.Sprintf("Failed to connect to couch. Error: %s.", err)
		return &Error{message}
	}

	if common.Configuration.CouchUseSSL {
		tlsConfig := &tls.Config{}
		if common.Configuration.CouchCACertificate != "" {
			var caFile string
			if strings.HasPrefix(common.Configuration.CouchCACertificate, "/") {
				caFile = common.Configuration.CouchCACertificate
			} else {
				caFile = common.Configuration.PersistenceRootPath + common.Configuration.CouchCACertificate
			}
			serverCaCert, err := ioutil.ReadFile(caFile)
			if err != nil {
				if _, ok := err.(*os.PathError); ok {
					serverCaCert = []byte(common.Configuration.CouchCACertificate)
					err = nil
				} else {
					message := fmt.Sprintf("Failed to find Couch SSL CA file. Error: %s.", err)
					return &Error{message}
				}
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(serverCaCert)
			tlsConfig.RootCAs = caCertPool
		}

		// Please avoid using this if possible! Makes using TLS pointless
		if common.Configuration.CouchAllowInvalidCertificates {
			tlsConfig.InsecureSkipVerify = true
		}

		setXport := couchdb.SetTransport(&http.Transport{TLSClientConfig: tlsConfig})
		err = client.Authenticate(context.TODO(), setXport)
		if err != nil {
			message := fmt.Sprintf("Authentication Failed. Error: %s.", err)
			return &Error{message}
		}
	}

	err = client.Authenticate(context.TODO(), &chttp.BasicAuth{Username: store.loginInfo["Username"], Password: store.loginInfo["Password"]})
	if err != nil {
		return err
	}

	available, err := client.Ping(context.TODO())
	if !available || err != nil {
		return err
	}

	exists, err := client.DBExists(context.TODO(), store.loginInfo["dbName"])
	if err != nil {
		return err
	}
	if !exists {
		client.CreateDB(context.TODO(), store.loginInfo["dbName"])
		if err != nil {
			return err
		}
	}
	store.connected = true
	store.client = client

	return nil
}

// Stop stops the CouchStorage store
func (store *CouchStorage) Stop() {
	store.client.Close(context.TODO())
}

// PerformMaintenance performs store's maintenance
func (store *CouchStorage) PerformMaintenance() {
	store.checkObjects()
}

// Cleanup erase the on disk Bolt database only for ESS and test
func (store *CouchStorage) Cleanup(isTest bool) common.SyncServiceError {
	return nil
}

// GetObjectsToActivate returns inactive objects that are ready to be activated
func (store *CouchStorage) GetObjectsToActivate() ([]common.MetaData, common.SyncServiceError) {
	currentTime := time.Now().UTC().Format(time.RFC3339)
	query := map[string]interface{}{"selector": map[string]interface{}{
		"metadata.inactive": true,
		"$or": []map[string]interface{}{
			map[string]interface{}{"status": common.NotReadyToSend},
			map[string]interface{}{"status": common.ReadyToSend}},
		"$and": []map[string]interface{}{
			map[string]interface{}{"metadata.activationTime": map[string]interface{}{"$ne": ""}},
			map[string]interface{}{"metadata.activationTime": map[string]interface{}{"$lte": currentTime}}}}}

	result := []couchObject{}
	if err := store.findAll(query, &result); err != nil {
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
func (store *CouchStorage) StoreObject(metaData common.MetaData, data []byte, status string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	id := getObjectCollectionID(metaData)
	var dests []common.StoreDestinationStatus
	var deletedDests []common.StoreDestinationStatus

	if metaData.DestinationPolicy != nil {
		metaData.DestinationPolicy.Timestamp = time.Now().UTC().UnixNano()
	}

	if status == common.NotReadyToSend || status == common.ReadyToSend {
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

	existingObject := &couchObject{}
	if err := store.getOne(id, existingObject); err != nil {
		if err != notFound {
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
		}
		if metaData.DestinationPolicy != nil {
			dests = existingObject.Destinations
		}
	}

	newObject := couchObject{ID: id, MetaData: metaData, Status: status, PolicyReceived: false,
		RemainingConsumers: metaData.ExpectedConsumers,
		RemainingReceivers: metaData.ExpectedConsumers, Destinations: dests}

	// In case of existing object, check if it had attachment and add to newObject if present
	// This is done only in case of metaOnly update
	// otherwise updated attachment will be added in the next block
	if existingObject != nil {
		newObject.Rev = existingObject.Rev
		if metaData.MetaOnly && data == nil {
			attachment, err := store.getAttachment(id)
			if err != nil {
				if err != notFound {
					return nil, err
				}
			} else {
				attachments := &kivik.Attachments{id: attachment}
				newObject.Attachments = attachments
			}
		}
	}

	// Add attachment to newObject for NoData=false
	if !metaData.NoData && data != nil {
		content := ioutil.NopCloser(bytes.NewReader(data))
		defer content.Close()

		attachment := &kivik.Attachment{ContentType: "application/octet-stream", Content: content}
		attachments := &kivik.Attachments{id: attachment}
		newObject.Attachments = attachments
	}

	// Cases where attachment needs to be removed are handled implicitly
	// by not adding the existing attachment to newObject
	if err := store.upsertObject(id, newObject); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to store an object. Error: %s.", err)}
	}

	return deletedDests, nil
}

// RetrieveObjectAndStatus returns the object meta data and status with the specified parameters
func (store *CouchStorage) RetrieveObjectAndStatus(orgID string, objectType string, objectID string) (*common.MetaData, string, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)

	if err := store.getOne(id, &result); err != nil {
		if err != notFound {
			return nil, "", &Error{fmt.Sprintf("Failed to retrieve object's status. Error: %s.", err)}
		}
		return nil, "", nil
	}
	return &result.MetaData, result.Status, nil
}

// DeleteStoredObject deletes the object
func (store *CouchStorage) DeleteStoredObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.deleteObject(id); err != nil {
		if err != notFound {
			return err
		}
	}
	return nil
}

// GetObjectDestinations gets destinations that the object has to be sent to
func (store *CouchStorage) GetObjectDestinations(metaData common.MetaData) ([]common.Destination, common.SyncServiceError) {
	result := &couchObject{}
	id := getObjectCollectionID(metaData)
	if err := store.getOne(id, &result); err != nil {
		switch err {
		case notFound:
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
func (store *CouchStorage) GetObjectDestinationsList(orgID string, objectType string,
	objectID string) ([]common.StoreDestinationStatus, common.SyncServiceError) {
	result := &couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		switch err {
		case notFound:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
		}
	}
	return result.Destinations, nil
}

// RetrieveObject returns the object meta data with the specified parameters
func (store *CouchStorage) RetrieveObject(orgID string, objectType string, objectID string) (*common.MetaData, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		switch err {
		case notFound:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to fetch the object. Error: %s.", err)}
		}
	}
	return &result.MetaData, nil
}

// RetrieveObjectStatus finds the object and return its status
func (store *CouchStorage) RetrieveObjectStatus(orgID string, objectType string, objectID string) (string, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		switch err {
		case notFound:
			return "", nil
		default:
			return "", &Error{fmt.Sprintf("Failed to retrieve object's status. Error: %s.", err)}
		}
	}
	return result.Status, nil
}

// RetrieveObjectRemainingConsumers finds the object and returns the number remaining consumers that
// haven't consumed the object yet
func (store *CouchStorage) RetrieveObjectRemainingConsumers(orgID string, objectType string, objectID string) (int, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		switch err {
		case notFound:
			return 0, nil
		default:
			return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining comsumers. Error: %s.", err)}
		}
	}
	return result.RemainingConsumers, nil
}

// DecrementAndReturnRemainingConsumers decrements the number of remaining consumers of the object
func (store *CouchStorage) DecrementAndReturnRemainingConsumers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining consumers. Error: %s.", err)}
	}
	result.RemainingConsumers = result.RemainingConsumers - 1
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to decrement object's remaining consumers. Error: %s.", err)}
	}
	return result.RemainingConsumers, nil
}

// ResetObjectRemainingConsumers sets the remaining consumers count to the original ExpectedConsumers value
func (store *CouchStorage) ResetObjectRemainingConsumers(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	result.RemainingConsumers = result.MetaData.ExpectedConsumers
	result.LastUpdate = time.Now()

	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to reset object's remaining comsumers. Error: %s.", err)}
	}
	return nil
}

// MarkObjectDeleted marks the object as deleted
func (store *CouchStorage) MarkObjectDeleted(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	result.Status = common.ObjDeleted
	result.MetaData.Deleted = true
	result.LastUpdate = time.Now()

	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to mark object as deleted. Error: %s.", err)}
	}
	return nil
}

// ActivateObject marks object as active
func (store *CouchStorage) ActivateObject(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	result.MetaData.Inactive = false
	result.LastUpdate = time.Now()

	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to mark object as active. Error: %s.", err)}
	}
	return nil
}

// UpdateObjectStatus updates object's status
func (store *CouchStorage) UpdateObjectStatus(orgID string, objectType string, objectID string, status string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	result.Status = status
	result.LastUpdate = time.Now()

	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to update object's status. Error: %s.", err)}
	}
	return nil
}

// RetrieveUpdatedObjects returns the list of all the edge updated objects that are not marked as consumed or received
// If received is true, return objects marked as received
func (store *CouchStorage) RetrieveUpdatedObjects(orgID string, objectType string, received bool) ([]common.MetaData, common.SyncServiceError) {
	result := []couchObject{}
	var query interface{}
	if received {
		query = map[string]interface{}{
			"selector": map[string]interface{}{
				"$or": []map[string]interface{}{
					map[string]interface{}{"status": common.CompletelyReceived},
					map[string]interface{}{"status": common.ObjReceived},
					map[string]interface{}{"status": common.ObjDeleted}},
				"metadata.destinationOrgID": orgID, "metadata.objectType": objectType}}
	} else {
		query = map[string]interface{}{
			"selector": map[string]interface{}{
				"$or": []map[string]interface{}{
					map[string]interface{}{"status": common.CompletelyReceived},
					map[string]interface{}{"status": common.ObjDeleted}},
				"metadata.destinationOrgID": orgID, "metadata.objectType": objectType}}
	}

	if err := store.findAll(query, &result); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
	}
	return metaDatas, nil
}

// RetrieveObjects returns the list of all the objects that need to be sent to the destination.
// Adds the new destination to the destinations lists of the relevant objects.
func (store *CouchStorage) RetrieveObjects(orgID string, destType string, destID string, resend int) ([]common.MetaData, common.SyncServiceError) {
	result := []couchObject{}
	query := map[string]interface{}{
		"selector": map[string]interface{}{"metadata.destinationOrgID": orgID,
			"$or": []map[string]interface{}{
				map[string]interface{}{"status": common.ReadyToSend},
				map[string]interface{}{"status": common.NotReadyToSend},
			}}}

	if err := store.findAll(query, &result); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
	}

	metaDatas := make([]common.MetaData, 0)
	for _, r := range result {
		if r.MetaData.DestinationPolicy != nil {
			continue
		}
		if (r.MetaData.DestType == "" || r.MetaData.DestType == destType) &&
			(r.MetaData.DestID == "" || r.MetaData.DestID == destID) {
			status := common.Pending
			if r.Status == common.ReadyToSend && !r.MetaData.Inactive {
				status = common.Delivering
			}
			needToUpdate := false
			// Add destination if it doesn't exist
			dest, err := store.RetrieveDestination(orgID, destType, destID)
			if err != nil {
				return nil, &Error{fmt.Sprintf("Failed to update object's destinations.")}
			}
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
				r.LastUpdate = time.Now()
				if err := store.upsertObject(r.ID, r); err != nil {
					return nil, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
				}
			}
		}
	}
	return metaDatas, nil
}

// UpdateObjectDeliveryStatus changes the object's delivery status and message for the destination
// Returns true if the status is Deleted and all the destinations are in status Deleted
func (store *CouchStorage) UpdateObjectDeliveryStatus(status string, message string, orgID string, objectType string, objectID string,
	destType string, destID string) (bool, common.SyncServiceError) {
	if status == "" && message == "" {
		return false, nil
	}
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	allDeleted := true

	if err := store.getOne(id, &result); err != nil {
		return false, &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	found := false
	allConsumed := true
	allDeleted = true
	for i, d := range result.Destinations {
		if !found && d.Destination.DestType == destType && d.Destination.DestID == destID {
			if message != "" || d.Status == common.Error {
				d.Message = message
			}
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

	result.LastUpdate = time.Now()
	if result.MetaData.AutoDelete && status == common.Consumed && allConsumed && result.MetaData.Expiration == "" {
		// Delete the object by setting its expiration time to one hour
		expirationTime := time.Now().Add(time.Hour * time.Duration(1)).UTC().Format(time.RFC3339)
		result.MetaData.Expiration = expirationTime
	}
	if err := store.upsertObject(id, result); err != nil {
		return false, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
	}
	return (allDeleted && status == common.Deleted), nil
}

// UpdateObjectDelivering marks the object as being delivered to all its destinations
func (store *CouchStorage) UpdateObjectDelivering(orgID string, objectType string, objectID string) common.SyncServiceError {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to retrieve object. Error: %s.", err)}
	}
	for i, d := range result.Destinations {
		d.Status = common.Delivering
		result.Destinations[i] = d
	}
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
	}
	return nil
}

// StoreDestination stores the destination
func (store *CouchStorage) StoreDestination(destination common.Destination) common.SyncServiceError {
	id := getDestinationCollectionID(destination)
	existingObject := &couchDestinationObject{}
	newObject := couchDestinationObject{ID: id, Destination: destination}
	if err := store.getOne(id, existingObject); err != nil {
		if err != notFound {
			return &Error{fmt.Sprintf("Failed to store a destination. Error: %s.", err)}
		}
		existingObject = nil
	}
	if existingObject != nil {
		newObject.Rev = existingObject.Rev
	}
	err := store.upsertObject(id, newObject)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to store a destination. Error: %s.", err)}
	}
	return nil
}

// RetrieveDestination retrieves a destination
func (store *CouchStorage) RetrieveDestination(orgID string, destType string, destID string) (*common.Destination, common.SyncServiceError) {
	result := couchDestinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.getOne(id, &result); err != nil {
		if err != notFound {
			return nil, &Error{fmt.Sprintf("Failed to fetch the destination. Error: %s.", err)}
		}
		return nil, &NotFound{fmt.Sprintf(" The destination %s:%s does not exist", destType, destID)}
	}
	return &result.Destination, nil
}

// RetrieveDestinations returns all the destinations with the provided orgID and destType
func (store *CouchStorage) RetrieveDestinations(orgID string, destType string) ([]common.Destination, common.SyncServiceError) {
	result := []couchDestinationObject{}
	var err error

	if orgID == "" {
		if destType == "" {
			err = store.findAll(map[string]interface{}{"selector": map[string]interface{}{"last-ping-time": map[string]interface{}{"$exists": true}}}, &result)
		} else {
			err = store.findAll(map[string]interface{}{"selector": map[string]interface{}{"destination.destinationType": destType}}, &result)
		}
	} else {
		if destType == "" {
			err = store.findAll(map[string]interface{}{"selector": map[string]interface{}{"destination.destinationOrgID": orgID}}, &result)
		} else {
			query := map[string]interface{}{
				"selector": map[string]interface{}{
					"destination.destinationOrgID": orgID,
					"destination.destinationType":  destType}}
			err = store.findAll(query, &result)
		}
	}
	if err != nil {
		return nil, &Error{fmt.Sprintf("Failed to fetch the destinations. Error: %s.", err)}
	}

	dests := make([]common.Destination, len(result))
	for i, r := range result {
		dests[i] = r.Destination
	}
	return dests, nil
}

//GetNumberOfStoredObjects returns the number of objects received from the application that are
//currently stored in this node's storage
func (store *CouchStorage) GetNumberOfStoredObjects() (uint32, common.SyncServiceError) {
	query := map[string]interface{}{"selector": map[string]interface{}{
		"$or": []map[string]interface{}{
			map[string]interface{}{"status": common.ReadyToSend},
			map[string]interface{}{"status": common.NotReadyToSend},
		}}}

	return store.countObjects(query)
}

// UpdateObjectDestinations updates object's destinations
// Returns the meta data, object's status, an array of deleted destinations, and an array of added destinations
func (store *CouchStorage) UpdateObjectDestinations(orgID string, objectType string, objectID string, destinationsList []string) (*common.MetaData, string,
	[]common.StoreDestinationStatus, []common.StoreDestinationStatus, common.SyncServiceError) {
	result := couchObject{}
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.getOne(id, &result); err != nil {
		return nil, "", nil, nil, &Error{fmt.Sprintf("Failed to retrieve object's destinations. Error: %s.", err)}
	}

	dests, deletedDests, addedDests, err := createDestinations(orgID, store, result.Destinations, destinationsList)
	if err != nil {
		return nil, "", nil, nil, err
	}

	result.Destinations = dests
	result.LastUpdate = time.Now()

	if err := store.upsertObject(id, result); err != nil {
		return nil, "", nil, nil, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
	}
	return &result.MetaData, result.Status, deletedDests, addedDests, nil
}

// DecrementAndReturnRemainingReceivers decrements the number of remaining receivers of the object
func (store *CouchStorage) DecrementAndReturnRemainingReceivers(orgID string, objectType string, objectID string) (int,
	common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to retrieve object's remaining receivers. Error: %s.", err)}
	}
	result.RemainingReceivers = result.RemainingReceivers - 1
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return 0, &Error{fmt.Sprintf("Failed to decrement object's remaining receivers. Error: %s.", err)}
	}
	return result.RemainingConsumers, nil
}

// RetrieveObjectsWithDestinationPolicy returns the list of all the objects that have a Destination Policy
// If received is true, return objects marked as policy received
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicy(orgID string, received bool) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	var query interface{}
	if received {
		query = map[string]interface{}{
			"metadata.destinationOrgID": orgID,
			"$and": []map[string]interface{}{
				map[string]interface{}{"status": map[string]interface{}{"$ne": common.ObjDeleted}},
				map[string]interface{}{"metadata.destinationPolicy": map[string]interface{}{"$ne": nil}},
			},
		}
	} else {
		query = map[string]interface{}{
			"metadata.destinationOrgID": orgID,
			"policy-received":           false,
			"$and": []map[string]interface{}{
				map[string]interface{}{"status": map[string]interface{}{"$ne": common.ObjDeleted}},
				map[string]interface{}{"metadata.destinationPolicy": map[string]interface{}{"$ne": nil}},
			},
		}
	}

	finalQuery := map[string]interface{}{"selector": query}
	return store.retrievePolicies(finalQuery)
}

// RetrieveObjectsWithDestinationPolicyByService returns the list of all the object Policies for a particular service
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	subquery := map[string]interface{}{
		"$elemMatch": map[string]interface{}{
			"orgID":       serviceOrgID,
			"serviceName": serviceName,
		},
	}
	query := map[string]interface{}{
		"metadata.destinationOrgID":           orgID,
		"metadata.destinationPolicy.services": subquery,
	}

	finalQuery := map[string]interface{}{"selector": query}
	return store.retrievePolicies(finalQuery)
}

// RetrieveObjectsWithDestinationPolicyUpdatedSince returns the list of all the objects that have a Destination Policy updated since the specified time
func (store *CouchStorage) RetrieveObjectsWithDestinationPolicyUpdatedSince(orgID string, since int64) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"metadata.destinationOrgID":            orgID,
			"metadata.destinationPolicy.timestamp": map[string]interface{}{"$gte": since},
		}}

	return store.retrievePolicies(query)
}

// RetrieveObjectsWithFilters returns the list of all the objects that meet the given conditions
func (store *CouchStorage) RetrieveObjectsWithFilters(orgID string, destinationPolicy *bool, dpServiceOrgID string, dpServiceName string, dpPropertyName string, since int64, objectType string, objectID string, destinationType string, destinationID string, noData *bool, expirationTimeBefore string) ([]common.MetaData, common.SyncServiceError) {
	result := []couchObject{}

	query := map[string]interface{}{
		"metadata.destinationOrgID": orgID,
	}
	if destinationPolicy != nil {
		if *destinationPolicy {
			query["metadata.destinationPolicy"] = map[string]interface{}{"$ne": nil}
			query["metadata.destinationPolicy.timestamp"] = map[string]interface{}{"$gte": since}

			if dpServiceOrgID != "" && dpServiceName != "" {
				subquery := map[string]interface{}{
					"$elemMatch": map[string]interface{}{
						"orgID":       dpServiceOrgID,
						"serviceName": dpServiceName,
					},
				}
				query["metadata.destinationPolicy.services"] = subquery
			}

			if dpPropertyName != "" {
				subquery := map[string]interface{}{
					"$elemMatch": map[string]interface{}{
						"name": dpPropertyName,
					},
				}
				query["metadata.destinationPolicy.properties"] = subquery
			}
		} else {
			query["metadata.destinationPolicy"] = nil
		}
	}

	if objectType != "" {
		query["metadata.objectType"] = objectType
		if objectID != "" {
			query["metadata.objectID"] = objectID
		}
	}

	if destinationType != "" {
		var subquery []map[string]interface{}
		if destinationID == "" {
			subquery = []map[string]interface{}{
				map[string]interface{}{"metadata.destinationType": destinationType},
				map[string]interface{}{"metadata.destinationsList": map[string]interface{}{"$allMatch": map[string]interface{}{"$regex": destinationType + ":*"}}},
			}
		} else {
			subquery = []map[string]interface{}{
				map[string]interface{}{"metadata.destinationType": destinationType, "metadata.destinationID": destinationID},
				map[string]interface{}{"metadata.destinationsList": map[string]interface{}{"$allMatch": map[string]interface{}{
					"$eq": destinationType + ":" + destinationID}},
				}}
		}
		query["$or"] = subquery
	}

	if noData != nil {
		query["metadata.noData"] = *noData
	}

	if expirationTimeBefore != "" {
		subquery := map[string]interface{}{
			"$ne":  "",
			"$lte": expirationTimeBefore,
		}
		query["metadata.expiration"] = subquery
	}

	finalQuery := map[string]interface{}{"selector": query}

	if err := store.findAll(finalQuery, &result); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to fetch the objects. Error: %s.", err)}
	}

	metaDatas := make([]common.MetaData, len(result))
	for i, r := range result {
		metaDatas[i] = r.MetaData
	}
	return metaDatas, nil
}

// RetrieveConsumedObjects returns all the consumed objects originated from this node
// ESS only API
func (store *CouchStorage) RetrieveConsumedObjects() ([]common.ConsumedObject, common.SyncServiceError) {
	return nil, nil
}

// RetrieveObjectData returns the object data with the specified parameters
func (store *CouchStorage) RetrieveObjectData(orgID string, objectType string, objectID string) (io.Reader, common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	attachment, err := store.getAttachment(id)
	if err != nil {
		switch err {
		case notFound:
			return nil, nil
		default:
			return nil, &Error{fmt.Sprintf("Failed to open file to read the data. Error: %s.", err)}
		}
	}

	data, err := ioutil.ReadAll(attachment.Content)
	defer attachment.Content.Close()
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(data), nil
}

// CloseDataReader closes the data reader if necessary
func (store *CouchStorage) CloseDataReader(dataReader io.Reader) common.SyncServiceError {
	return nil
}

// ReadObjectData returns the object data with the specified parameters
func (store *CouchStorage) ReadObjectData(orgID string, objectType string, objectID string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	attachment, err := store.getAttachment(id)
	if err != nil {
		if err == notFound {
			return nil, true, 0, notFound
		}
		return nil, true, 0, err
	}

	data, err := ioutil.ReadAll(attachment.Content)
	defer attachment.Content.Close()
	if err != nil {
		return nil, true, 0, err
	}

	lod := int64(len(data))
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
	copy(b, data[offset:])

	return b, eof, int(s), nil
}

// StoreObjectData stores object's data
// Return true if the object was found and updated
// Return false and no error, if the object doesn't exist
func (store *CouchStorage) StoreObjectData(orgID string, objectType string, objectID string, dataReader io.Reader) (bool, common.SyncServiceError) {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := &couchObject{}
	if err := store.getOne(id, result); err != nil {
		switch err {
		case notFound:
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
		result.MetaData.DataID = newID
		result.MetaData.InstanceID = newID
		result.LastUpdate = time.Now()
	}

	size, err := store.addAttachment(id, dataReader)
	if err != nil {
		return false, err
	}

	// need to get result object again because attachment has been added
	updatedResult := &couchObject{}
	if err := store.getOne(id, updatedResult); err != nil {
		return false, &Error{fmt.Sprintf("Failed to store the data. Error: %s.", err)}
	}

	// Update object size
	updatedResult.MetaData.ObjectSize = size

	if err := store.upsertObject(id, updatedResult); err != nil {
		return false, &Error{fmt.Sprintf("Failed to update object's size. Error: %s.", err)}
	}

	return true, nil
}

// AppendObjectData appends a chunk of data to the object's data
func (store *CouchStorage) AppendObjectData(orgID string, objectType string, objectID string, dataReader io.Reader,
	dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	attachment, err := store.getAttachment(id)
	if err != nil {
		if err == notFound {
			return notFound
		}
		return err
	}

	objectData, err := ioutil.ReadAll(attachment.Content)
	defer attachment.Content.Close()
	if err != nil {
		return err
	}
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
		objectData = make([]byte, total)
	} else {
		objectData = ensureArrayCapacity(objectData, total)
	}
	if data != nil {
		copy(objectData[offset:], data)
	} else {
		count, err := dataReader.Read(objectData[offset:])
		if err != nil {
			return &Error{"Failed to read object data. Error: " + err.Error()}
		}
		if count != int(dataLength) {
			return &Error{fmt.Sprintf("Read %d bytes for the object data, instead of %d", count, dataLength)}
		}
	}

	if _, err := store.addAttachment(id, bytes.NewReader(objectData)); err != nil {
		return err
	}
	return nil
}

// UpdateObjectSourceDataURI updates object's source data URI
func (store *CouchStorage) UpdateObjectSourceDataURI(orgID string, objectType string, objectID string, sourceDataURI string) common.SyncServiceError {
	return nil
}

// MarkDestinationPolicyReceived marks an object's destination policy as having been received
func (store *CouchStorage) MarkDestinationPolicyReceived(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	result := couchObject{}
	if err := store.getOne(id, &result); err != nil {
		return &Error{fmt.Sprintf("Failed to mark an object's destination policy as received. Error: %s", err)}
	}
	result.PolicyReceived = true
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to mark an object's destination policy as received. Error: %s", err)}
	}
	return nil
}

// DeleteStoredData deletes the object's data
func (store *CouchStorage) DeleteStoredData(orgID string, objectType string, objectID string) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	if err := store.removeAttachment(id); err != nil {
		return err
	}
	return nil
}

// CleanObjects removes the objects received from the other side.
// For persistant storage only partially recieved objects are removed.
func (store *CouchStorage) CleanObjects() common.SyncServiceError {
	// ESS only function
	return nil
}

// AddWebhook stores a webhook for an object type
func (store *CouchStorage) AddWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Adding a webhook for %s\n", id)
	}
	result := &couchWebhookObject{}
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
			result.Hooks = make([]string, 0)
			result.Hooks = append(result.Hooks, url)
			result.ID = id
			if err = store.upsertObject(id, result); err != nil {
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
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to add a webhook. Error: %s.", err)}
	}
	return nil
}

// DeleteWebhook deletes a webhook for an object type
func (store *CouchStorage) DeleteWebhook(orgID string, objectType string, url string) common.SyncServiceError {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Deleting a webhook for %s\n", id)
	}
	result := &couchWebhookObject{}
	if err := store.getOne(id, &result); err != nil {
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
	result.LastUpdate = time.Now()
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to delete a webhook. Error: %s.", err)}
	}
	return nil
}

// RetrieveWebhooks gets the webhooks for the object type
func (store *CouchStorage) RetrieveWebhooks(orgID string, objectType string) ([]string, common.SyncServiceError) {
	id := orgID + ":" + objectType
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving a webhook for %s\n", id)
	}
	result := &couchWebhookObject{}
	if err := store.getOne(id, &result); err != nil {
		return nil, err
	}

	if len(result.Hooks) == 0 {
		return nil, &NotFound{"No webhooks"}
	}
	return result.Hooks, nil
}

// DestinationExists returns true if the destination exists, and false otherwise
func (store *CouchStorage) DestinationExists(orgID string, destType string, destID string) (bool, common.SyncServiceError) {
	result := couchDestinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DeleteDestination deletes the destination
func (store *CouchStorage) DeleteDestination(orgID string, destType string, destID string) common.SyncServiceError {
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.deleteObject(id); err != nil {
		return &Error{fmt.Sprintf("Failed to delete destination. Error: %s.", err)}
	}
	return nil
}

// UpdateDestinationLastPingTime updates the last ping time for the destination
func (store *CouchStorage) UpdateDestinationLastPingTime(destination common.Destination) common.SyncServiceError {
	id := getDestinationCollectionID(destination)
	result := couchDestinationObject{}
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
			return notFound
		}
		return &Error{fmt.Sprintf("Failed to update the last ping time for destination. Error: %s\n", err)}
	}

	result.LastPingTime = time.Now()
	err := store.upsertObject(id, result)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to update the last ping time for destination. Error: %s\n", err)}
	}
	return nil
}

// RemoveInactiveDestinations removes destinations that haven't sent ping since the provided timestamp
func (store *CouchStorage) RemoveInactiveDestinations(lastTimestamp time.Time) {

	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"last-ping-time": map[string]interface{}{"$lte": lastTimestamp}},
		"fields": []string{"destination"},
	}
	dests := []couchDestinationObject{}
	if err := store.findAll(query, &dests); err != nil {
		if err != notFound && log.IsLogging(logger.ERROR) {
			log.Error("Error in couchStorage.RemoveInactiveDestinations: failed to remove inactive destinations. Error: %s\n", err)
		}
		return
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Removing inactive destinations")
	}
	for _, d := range dests {
		if err := store.DeleteNotificationRecords(d.Destination.DestOrgID, "", "", d.Destination.DestType, d.Destination.DestID); err != nil &&
			err != notFound && log.IsLogging(logger.ERROR) {
			log.Error("Error in couchStorage.RemoveInactiveDestinations: failed to remove notifications for inactive destinations. Error: %s\n", err)
		}
		if err := store.DeleteDestination(d.Destination.DestOrgID, d.Destination.DestType, d.Destination.DestID); err != nil &&
			err != notFound && log.IsLogging(logger.ERROR) {
			log.Error("Error in couchStorage.RemoveInactiveDestinations: failed to remove inactive destination. Error: %s\n", err)
		}
	}
}

// GetNumberOfDestinations returns the number of currently registered ESS nodes (for CSS)
// This function needs to be checked
func (store *CouchStorage) GetNumberOfDestinations() (uint32, common.SyncServiceError) {

	query := map[string]interface{}{"selector": map[string]interface{}{
		"$or": []map[string]interface{}{
			map[string]interface{}{"destination.communication": common.MQTTProtocol},
			map[string]interface{}{"destination.communication": common.HTTPProtocol},
		}}}

	return store.countObjects(query)
}

// RetrieveDestinationProtocol retrieves the communication protocol for the destination
func (store *CouchStorage) RetrieveDestinationProtocol(orgID string, destType string, destID string) (string, common.SyncServiceError) {
	result := couchDestinationObject{}
	id := createDestinationCollectionID(orgID, destType, destID)
	if err := store.getOne(id, &result); err != nil {
		return "", &Error{fmt.Sprintf("Failed to fetch the destination. Error: %s.", err)}
	}
	return result.Destination.Communication, nil
}

// RetrieveAllObjects retrieves All Objects
func (store *CouchStorage) RetrieveAllObjects(orgID string, objectType string) ([]common.ObjectDestinationPolicy, common.SyncServiceError) {
	query := map[string]interface{}{"selector": map[string]interface{}{
		"metadata.destinationOrgID": orgID,
		"metadata.objectType":       objectType,
	}}
	return store.retrievePolicies(query)
}

// GetObjectsForDestination retrieves objects that are in use on a given node
func (store *CouchStorage) GetObjectsForDestination(orgID string, destType string, destID string) ([]common.ObjectStatus, common.SyncServiceError) {
	notificationRecords := []couchNotificationObject{}
	query := map[string]interface{}{
		"selector": map[string]interface{}{
			"$or": []map[string]interface{}{
				map[string]interface{}{"notification.status": common.Update},
				map[string]interface{}{"notification.status": common.UpdatePending},
				map[string]interface{}{"notification.status": common.Updated},
				map[string]interface{}{"notification.status": common.ReceivedByDestination},
				map[string]interface{}{"notification.status": common.ConsumedByDestination},
				map[string]interface{}{"notification.status": common.Error}},
			"notification.destinationOrgID": orgID,
			"notification.destinationID":    destID,
			"notification.destinationType":  destType}}

	if err := store.findAll(query, &notificationRecords); err != nil && err != notFound {
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
func (store *CouchStorage) RetrieveAllObjectsAndUpdateDestinationListForDestination(destOrgID string, destType string, destID string) ([]common.MetaData, common.SyncServiceError) {
	result := []couchObject{}

	query := map[string]interface{}{}
	subquery := map[string]interface{}{
		"$elemMatch": map[string]interface{}{
			"destination.destinationOrgID": destOrgID,
			"destination.destinationType":  destType,
			"destination.destinationID":    destID,
		},
	}
	query["destinations"] = subquery
	finalQuery := map[string]interface{}{"selector": query}

	if err := store.findAll(finalQuery, &result); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to fetch the objects for destination %s %s %s from storage. Error: %s.", destOrgID, destType, destID, err)}
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

		r.Destinations = updatedDestinationList
		r.LastUpdate = time.Now()

		if err := store.upsertObject(r.ID, r); err != nil {
			emptyMeta := make([]common.MetaData, 0)
			return emptyMeta, &Error{fmt.Sprintf("Failed to update object's destinations. Error: %s.", err)}
		}
	}
	return metaDatas, nil
}

// RetrieveObjectAndRemovedDestinationPolicyServices returns the object metadata and removedDestinationPolicyServices with the specified param, only for ESS
func (store *CouchStorage) RetrieveObjectAndRemovedDestinationPolicyServices(orgID string, objectType string, objectID string) (*common.MetaData, []common.ServiceID, common.SyncServiceError) {
	removedDestinationPolicyServices := []common.ServiceID{}
	return nil, removedDestinationPolicyServices, nil
}

// UpdateRemovedDestinationPolicyServices update the removedDestinationPolicyServices, only for ESS
func (store *CouchStorage) UpdateRemovedDestinationPolicyServices(orgID string, objectType string, objectID string, destinationPolicyServices []common.ServiceID) common.SyncServiceError {
	return nil
}

// UpdateNotificationRecord updates/adds a notification record to the object
func (store *CouchStorage) UpdateNotificationRecord(notification common.Notification) common.SyncServiceError {
	id := getNotificationCollectionID(&notification)
	if notification.ResendTime == 0 {
		resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)
		notification.ResendTime = resendTime
	}

	existingObject := &couchNotificationObject{}
	if err := store.getOne(id, existingObject); err != nil {
		if err != notFound {
			return &Error{fmt.Sprintf("Failed to update notification record. Error: %s.", err)}
		}
		existingObject = nil
	}

	newObject := couchNotificationObject{ID: id, Notification: notification}
	if existingObject != nil {
		newObject.Rev = existingObject.Rev
	}
	err := store.upsertObject(id, newObject)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to update notification record. Error: %s.", err)}
	}
	return nil
}

// UpdateNotificationResendTime sets the resend time of the notification to common.Configuration.ResendInterval*6
func (store *CouchStorage) UpdateNotificationResendTime(notification common.Notification) common.SyncServiceError {
	id := getNotificationCollectionID(&notification)
	resendTime := time.Now().Unix() + int64(common.Configuration.ResendInterval*6)

	result := &couchNotificationObject{}
	if err := store.getOne(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to update notification resend time. Error: %s.", err)}
	}

	result.Notification.ResendTime = resendTime
	if err := store.upsertObject(id, result); err != nil {
		return &Error{fmt.Sprintf("Failed to update notification resend time. Error: %s.", err)}
	}
	return nil
}

// RetrieveNotificationRecord retrieves notification
func (store *CouchStorage) RetrieveNotificationRecord(orgID string, objectType string, objectID string, destType string,
	destID string) (*common.Notification, common.SyncServiceError) {
	id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
	result := couchNotificationObject{}
	if err := store.getOne(id, &result); err != nil {
		if err == notFound {
			return nil, nil
		}
		return nil, &Error{fmt.Sprintf("Failed to fetch the notification. Error: %s.", err)}
	}
	return &result.Notification, nil
}

// DeleteNotificationRecords deletes notification records to an object
//very different
func (store *CouchStorage) DeleteNotificationRecords(orgID string, objectType string, objectID string, destType string, destID string) common.SyncServiceError {
	var err error
	if objectType != "" && objectID != "" {
		if destType != "" && destID != "" {
			id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
			err = store.deleteObject(id)
		} else {
			query := map[string]interface{}{
				"selector": map[string]interface{}{
					"notification.destinationOrgID": orgID,
					"notification.objectType":       objectType,
					"notification.objectID":         objectID,
				},
				"fields": []string{"_id", "_rev"},
			}
			if err := store.deleteAllNotificationObjects(query); err != nil {
				return err
			}
		}
	} else {
		query := map[string]interface{}{
			"selector": map[string]interface{}{
				"notification.destinationOrgID": orgID,
				"notification.destinationType":  destType,
				"notification.destinationID":    destID,
			},
			"fields": []string{"_id", "_rev"},
		}
		if err := store.deleteAllNotificationObjects(query); err != nil {
			return err
		}
	}

	if err != nil && err != notFound {
		return &Error{fmt.Sprintf("Failed to delete notification records. Error: %s.", err)}
	}
	return nil
}

// RetrieveNotifications returns the list of all the notifications that need to be resent to the destination
func (store *CouchStorage) RetrieveNotifications(orgID string, destType string, destID string, retrieveReceived bool) ([]common.Notification, common.SyncServiceError) {
	result := []couchNotificationObject{}
	var query map[string]interface{}
	if destType == "" && destID == "" {
		currentTime := time.Now().Unix()

		query = map[string]interface{}{"$or": []map[string]interface{}{
			map[string]interface{}{"notification.status": common.Getdata},
			map[string]interface{}{
				"notification.resendTime": map[string]interface{}{"$lte": currentTime},
				"$or": []map[string]interface{}{
					map[string]interface{}{"notification.status": common.Update},
					map[string]interface{}{"notification.status": common.Received},
					map[string]interface{}{"notification.status": common.Consumed},
					map[string]interface{}{"notification.status": common.Getdata},
					map[string]interface{}{"notification.status": common.Delete},
					map[string]interface{}{"notification.status": common.Deleted}}}}}
	} else {
		if retrieveReceived {
			query = map[string]interface{}{"$or": []map[string]interface{}{
				map[string]interface{}{"notification.status": common.Update},
				map[string]interface{}{"notification.status": common.Received},
				map[string]interface{}{"notification.status": common.Consumed},
				map[string]interface{}{"notification.status": common.Getdata},
				map[string]interface{}{"notification.status": common.Data},
				map[string]interface{}{"notification.status": common.ReceivedByDestination},
				map[string]interface{}{"notification.status": common.Delete},
				map[string]interface{}{"notification.status": common.Deleted}},
				"notification.destinationOrgID": orgID,
				"notification.destinationID":    destID,
				"notification.destinationType":  destType}
		} else {
			query = map[string]interface{}{"$or": []map[string]interface{}{
				map[string]interface{}{"notification.status": common.Update},
				map[string]interface{}{"notification.status": common.Received},
				map[string]interface{}{"notification.status": common.Consumed},
				map[string]interface{}{"notification.status": common.Getdata},
				map[string]interface{}{"notification.status": common.Delete},
				map[string]interface{}{"notification.status": common.Deleted}},
				"notification.destinationOrgID": orgID,
				"notification.destinationID":    destID,
				"notification.destinationType":  destType}
		}
	}

	finalQuery := map[string]interface{}{"selector": query}
	if err := store.findAll(finalQuery, &result); err != nil && err != notFound {
		return nil, &Error{fmt.Sprintf("Failed to fetch the notifications. Error: %s.", err)}
	}

	notifications := make([]common.Notification, 0)
	for _, n := range result {
		notifications = append(notifications, n.Notification)
	}
	return notifications, nil
}

// RetrievePendingNotifications returns the list of pending notifications that are waiting to be sent to the destination
func (store *CouchStorage) RetrievePendingNotifications(orgID string, destType string, destID string) ([]common.Notification, common.SyncServiceError) {
	result := []couchNotificationObject{}
	var query map[string]interface{}

	if destType == "" && destID == "" {
		query = map[string]interface{}{"$or": []map[string]interface{}{
			map[string]interface{}{"notification.status": common.UpdatePending},
			map[string]interface{}{"notification.status": common.ConsumedPending},
			map[string]interface{}{"notification.status": common.DeletePending},
			map[string]interface{}{"notification.status": common.DeletedPending}},
			"notification.destinationOrgID": orgID}
	} else {
		query = map[string]interface{}{"$or": []map[string]interface{}{
			map[string]interface{}{"notification.status": common.UpdatePending},
			map[string]interface{}{"notification.status": common.ConsumedPending},
			map[string]interface{}{"notification.status": common.DeletePending},
			map[string]interface{}{"notification.status": common.DeletedPending}},
			"notification.destinationOrgID": orgID,
			"notification.destinationID":    destID,
			"notification.destinationType":  destType}
	}
	if err := store.findAll(map[string]interface{}{"selector": query}, &result); err != nil && err != notFound {
		return nil, &Error{fmt.Sprintf("Failed to fetch the notifications. Error: %s.", err)}
	}

	notifications := make([]common.Notification, 0)
	for _, n := range result {
		notifications = append(notifications, n.Notification)
	}
	return notifications, nil
}

// InsertInitialLeader inserts the initial leader document if the collection is empty
func (store *CouchStorage) InsertInitialLeader(leaderID string) (bool, common.SyncServiceError) {
	return true, nil
}

// LeaderPeriodicUpdate does the periodic update of the leader document by the leader
func (store *CouchStorage) LeaderPeriodicUpdate(leaderID string) (bool, common.SyncServiceError) {
	return false, nil
}

// RetrieveLeader retrieves the Heartbeat timeout and Last heartbeat time stamp from the leader document
func (store *CouchStorage) RetrieveLeader() (string, int32, time.Time, int64, common.SyncServiceError) {
	return "", 0, time.Now(), 0, nil
}

// UpdateLeader updates the leader entry for a leadership takeover
func (store *CouchStorage) UpdateLeader(leaderID string, version int64) (bool, common.SyncServiceError) {
	return false, nil
}

// ResignLeadership causes this sync service to give up the Leadership
func (store *CouchStorage) ResignLeadership(leaderID string) common.SyncServiceError {
	return nil
}

// RetrieveTimeOnServer retrieves the current time on the database server
func (store *CouchStorage) RetrieveTimeOnServer() (time.Time, error) {
	return time.Now(), nil
}

// StoreOrgToMessagingGroup inserts organization to messaging groups table
func (store *CouchStorage) StoreOrgToMessagingGroup(orgID string, messagingGroup string) common.SyncServiceError {

	existingObject := &couchMessagingGroupObject{}
	if err := store.getOne(orgID, existingObject); err != nil {
		if err != notFound {
			return err
		}
		existingObject = nil
	}

	newObject := couchMessagingGroupObject{ID: orgID, GroupName: messagingGroup}
	if existingObject != nil {
		newObject.Rev = existingObject.Rev
	}
	err := store.upsertObject(orgID, newObject)
	if err != nil {
		return &Error{fmt.Sprintf("Failed to store organization's messaging group. Error: %s.", err)}
	}
	return nil
}

// DeleteOrgToMessagingGroup deletes organization from messaging groups table
func (store *CouchStorage) DeleteOrgToMessagingGroup(orgID string) common.SyncServiceError {
	if err := store.deleteObject(orgID); err != nil && err != notFound {
		return err
	}
	return nil
}

// RetrieveMessagingGroup retrieves messaging group for organization
func (store *CouchStorage) RetrieveMessagingGroup(orgID string) (string, common.SyncServiceError) {
	result := couchMessagingGroupObject{}
	if err := store.getOne(orgID, &result); err != nil {
		if err != notFound {
			return "", err
		}
		return "", nil
	}
	return result.GroupName, nil
}

// RetrieveUpdatedMessagingGroups retrieves messaging groups that were updated after the specified time
func (store *CouchStorage) RetrieveUpdatedMessagingGroups(time time.Time) ([]common.MessagingGroup,
	common.SyncServiceError) {
	result := []couchMessagingGroupObject{}
	if err := store.findAll(map[string]interface{}{"selector": map[string]interface{}{"last-update": map[string]interface{}{"$gte": time}}}, &result); err != nil {
		return nil, err
	}
	groups := make([]common.MessagingGroup, 0)
	for _, group := range result {
		groups = append(groups, common.MessagingGroup{OrgID: group.ID, GroupName: group.GroupName})
	}
	return groups, nil
}

// DeleteOrganization cleans up the storage from all the records associated with the organization
func (store *CouchStorage) DeleteOrganization(orgID string) common.SyncServiceError {
	if err := store.DeleteOrgToMessagingGroup(orgID); err != nil {
		return err
	}

	if err := store.deleteAllDestinationObjects(map[string]interface{}{"selector": map[string]interface{}{"destination.destinationOrgID": orgID}}); err != nil && err != notFound {
		return &Error{fmt.Sprintf("Failed to delete destinations. Error: %s.", err)}
	}

	if err := store.deleteAllNotificationObjects(map[string]interface{}{"selector": map[string]interface{}{"notification.destinationOrgID": orgID}}); err != nil && err != notFound {
		return &Error{fmt.Sprintf("Failed to delete notifications. Error: %s.", err)}
	}

	if err := store.deleteAllCouchObjects(map[string]interface{}{"selector": map[string]interface{}{"metadata.destinationOrgID": orgID}}); err != nil && err != notFound {
		return &Error{fmt.Sprintf("Failed to delete objects. Error: %s.", err)}
	}

	return nil
}

// IsConnected returns false if the storage cannont be reached, and true otherwise
func (store *CouchStorage) IsConnected() bool {
	return store.connected
}

// StoreOrganization stores organization information
// Returns the stored record timestamp for multiple CSS updates
func (store *CouchStorage) StoreOrganization(org common.Organization) (time.Time, common.SyncServiceError) {

	existingObject := &couchOrganizationObject{}
	if err := store.getOne(org.OrgID, existingObject); err != nil {
		if err != notFound {
			return time.Now(), err
		}
		existingObject = nil
	}

	newObject := couchOrganizationObject{ID: org.OrgID, Organization: org}
	if existingObject != nil {
		newObject.Rev = existingObject.Rev
	}

	err := store.upsertObject(org.OrgID, newObject)
	if err != nil {
		return time.Now(), &Error{fmt.Sprintf("Failed to store organization's info. Error: %s.", err)}
	}

	return newObject.LastUpdate, nil
}

// RetrieveOrganizationInfo retrieves organization information
func (store *CouchStorage) RetrieveOrganizationInfo(orgID string) (*common.StoredOrganization, common.SyncServiceError) {
	result := couchOrganizationObject{}
	if err := store.getOne(orgID, &result); err != nil {
		if err != notFound {
			return nil, err
		}
		return nil, nil
	}
	return &common.StoredOrganization{Org: result.Organization, Timestamp: result.LastUpdate}, nil
}

// DeleteOrganizationInfo deletes organization information
func (store *CouchStorage) DeleteOrganizationInfo(orgID string) common.SyncServiceError {
	if err := store.deleteObject(orgID); err != nil && err != notFound {
		return err
	}
	return nil
}

// RetrieveOrganizations retrieves stored organizations' info
func (store *CouchStorage) RetrieveOrganizations() ([]common.StoredOrganization, common.SyncServiceError) {
	result := []couchOrganizationObject{}
	query := map[string]interface{}{"selector": map[string]interface{}{"org": map[string]interface{}{"$exists": true}}}
	if err := store.findAll(query, &result); err != nil {
		return nil, err
	}
	orgs := make([]common.StoredOrganization, 0)
	for _, org := range result {
		orgs = append(orgs, common.StoredOrganization{Org: org.Organization, Timestamp: org.LastUpdate})
	}
	return orgs, nil
}

// RetrieveUpdatedOrganizations retrieves organizations that were updated after the specified time
func (store *CouchStorage) RetrieveUpdatedOrganizations(time time.Time) ([]common.StoredOrganization, common.SyncServiceError) {
	result := []couchOrganizationObject{}
	if err := store.findAll(map[string]interface{}{"selector": map[string]interface{}{"last-update": map[string]interface{}{"$gte": time}}}, &result); err != nil {
		return nil, err
	}
	orgs := make([]common.StoredOrganization, 0)
	for _, org := range result {
		orgs = append(orgs, common.StoredOrganization{Org: org.Organization, Timestamp: org.LastUpdate})
	}
	return orgs, nil
}

// AddUsersToACL adds users to an ACL
func (store *CouchStorage) AddUsersToACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.addUsersToACLHelper(aclType, orgID, key, users)
}

// RemoveUsersFromACL removes users from an ACL
func (store *CouchStorage) RemoveUsersFromACL(aclType string, orgID string, key string, users []common.ACLentry) common.SyncServiceError {
	return store.removeUsersFromACLHelper(aclType, orgID, key, users)
}

// RetrieveACL retrieves the list of usernames on an ACL
func (store *CouchStorage) RetrieveACL(aclType string, orgID string, key string, aclUserType string) ([]common.ACLentry, common.SyncServiceError) {
	return store.retrieveACLHelper(aclType, orgID, key, aclUserType)
}

// RetrieveACLsInOrg retrieves the list of ACLs in an organization
func (store *CouchStorage) RetrieveACLsInOrg(aclType string, orgID string) ([]string, common.SyncServiceError) {
	return store.retrieveACLsInOrgHelper(aclType, orgID)
}

// RetrieveObjOrDestTypeForGivenACLUser retrieves object types that given acl user has access to
func (store *CouchStorage) RetrieveObjOrDestTypeForGivenACLUser(aclType string, orgID string, aclUserType string, aclUsername string, aclRole string) ([]string, common.SyncServiceError) {
	return store.retrieveObjOrDestTypeForGivenACLUserHelper(aclType, orgID, aclUserType, aclUsername, aclRole)
}

// IsPersistent returns true if the storage is persistent, and false otherwise
func (store *CouchStorage) IsPersistent() bool {
	return true
}
