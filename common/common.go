package common

import (
	"strings"
	"time"
)

// SyncServiceError is a common error type used in the sync service
// swagger:ignore
type SyncServiceError error

// InvalidRequest is the error for invalid reguests
// swagger:ignore
type InvalidRequest struct {
	Message string
}

func (e *InvalidRequest) Error() string {
	return e.Message
}

// SetupError is the error for setup issues
// swagger:ignore
type SetupError struct {
	Message string
}

func (e *SetupError) Error() string {
	return e.Message
}

// Destination describes a sync service node.
// Each sync service edge node (ESS) has an address that is composed of the node's ID, Type, and Organization.
// An ESS node communicates with the CSS using either MQTT or HTTP.
// swagger:model
type Destination struct {

	// DestOrgID is the destination organization ID
	// Each sync service object belongs to a single organization
	DestOrgID string `json:"destinationOrgID" bson:"destination-org-id"`

	// DestType is the destination type
	//   required: true
	DestType string `json:"destinationType" bson:"destination-type"`

	// DestID is the destination ID
	//   required: true
	DestID string `json:"destinationID" bson:"destination-id"`

	// Communication is the communication protocol used by the destination to connect (can be MQTT or HTTP)
	//   required: true
	Communication string `json:"communication" bson:"communication"`
}

// MetaData is the metadata that identifies and defines the sync service object.
// Every object includes metadata (mandatory) and data (optional). The metadata and data can be updated independently.
// Each sync service node (ESS) has an address that is composed of the node's ID, Type, and Organization.
// To send an object to a single node set the destinationType and destinationID fields to match the node's Type and ID.
// To send an object to all the nodes of a certain type set destinationType to the appropriate type and leave destinationID empty.
// If both destinationType and destinationID are empty the object is sent to all nodes.
// swagger:model
type MetaData struct {
	// ObjectID is a unique identifier of the object
	//   required: true
	ObjectID string `json:"objectID" bson:"object-id"`

	// ObjectType is the type of the object.
	// The type is used to group multiple objects, for example when checking for object updates.
	//   required: true
	ObjectType string `json:"objectType" bson:"object-type"`

	// DestOrgID is the organization ID of the object (an object belongs to exactly one organization).
	// This field is ignored when working with ESS (the ESS's orgID is used).
	DestOrgID string `json:"destinationOrgID" bson:"destination-org-id"`

	// DestID is the ID of the destination. If omitted the object is sent to all ESSs with the same DestType.
	// This field is ignored when working with ESS (the destination is the CSS).
	DestID string `json:"destinationID" bson:"destination-id"`

	// DestType is the type of destination to send the object to.
	// If omitted (and if DestinationsList is omitted too) the object is broadcasted to all known destinations.
	// This field is ignored when working with ESS (the destination is always the CSS).
	DestType string `json:"destinationType" bson:"destination-type"`

	// DestinationsList is the list of destinations as type:id pairs to send the object to.
	// When a DestinationsList is provided DestType and DestID must be omitted.
	// This field is ignored when working with ESS (the destination is always the CSS).
	DestinationsList []string `json:"destinationsList" bson:"destinations-list"`

	// Expiration is a timestamp/date indicating when the object expires.
	// When the object expires it is automatically deleted.
	// The timestamp should be provided in RFC3339 format.
	// Optional field, if omitted the object doesn't expire.
	Expiration string `json:"expiration" bson:"expiration"`

	// Version is the object's version (as used by the application).
	// Optional field, empty by default.
	Version string `json:"version" bson:"version"`

	// Description is a textual description of the object.
	// Optional field, empty by default.
	Description string `json:"description" bson:"description"`

	// Link is a link to where the data for this object can be fetched from.
	// The link is set and used by the application. The sync service does not access the link.
	// Optional field, if omitted the data must be provided by the application.
	Link string `json:"link" bson:"link"`

	// Inactive is a flag indicating that this object is inactive for now.
	// An object can be created as inactive which means it is not delivered to its destination. The object can be activated later.
	// Optional field, default is false (object active).
	Inactive bool `json:"inactive" bson:"inactive"`

	// ActivationTime is a timestamp/date as to when this object should automatically be activated.
	// The timestamp should be provided in RFC3339 format.
	// Optional field, if omitted (and Inactive is true) the object is never automatically activated.
	ActivationTime string `json:"activationTime" bson:"activation-time"`

	// NoData is a flag indicating that there is no data for this object.
	// Objects with no data can be used, for example, to send notifications.
	// Optional field, default is false (object includes data).
	NoData bool `json:"noData" bson:"no-data"`

	// MetaOnly is a flag that indicates that this update is only of the metadata. The current object's data is left unchanged.
	// Optional field, default is false (both data and metadata are updated).
	MetaOnly bool `json:"metaOnly" bson:"meta-only"`

	// DestinationDataURI is a URI indicating where the receiver of the object should store it.
	// Currently only file URIs are supported.
	// This field is available only when working with the CSS.
	// Optional field, if omitted the object is stored in the node's internal storage.
	DestinationDataURI string `json:"destinationDataUri" bson:"data-uri"`

	// SourceDataURI is a URI indicating where the sender of the object should read the data from.
	// Currently only file URIs are supported.
	// This field is available only when working with the ESS.
	// Optional field, if omitted the object's data should be provided by the user.
	SourceDataURI string `json:"sourceDataUri" bson:"source-data-uri"`

	// ExpectedConsumers is the number of applications that are expected to indicate that they have consumed the object.
	// Optional field, default is 1.
	ExpectedConsumers int `json:"consumers" bson:"consumers"`

	// AutoDelete is a flag indicating whether to delete the object after it is delivered to all its destinations from the DestinationsList.
	// Optional field, default is false (do not delete).
	// This field is used only when working with the CSS. Objects are always deleted after delivery on the ESS.
	AutoDelete bool `json:"autodelete" bson:"autodelete"`

	// OriginID is the ID of origin of the object. Set by the internal code.
	// Read only field, should not be set by users.
	OriginID string `json:"originID" bson:"origin-id"`

	// Deleted is a flag indicating to applications polling for updates that this object has been deleted.
	// Read only field, should not be set by users.
	Deleted bool `json:"deleted" bson:"deleted"`

	// OriginType is the type of origin of the object. Set by the internal code.
	// Read only field, should not be set by users.
	OriginType string `json:"originType" bson:"origin-type"`

	// InstanceID is an internal instance ID.
	// This field should not be set by users.
	InstanceID int64 `json:"instanceID" bson:"instance-id"`

	// ObjectSize is an internal field indicating the size of the object's data.
	// This field should not be set by users.
	ObjectSize int64 `json:"objectSize" bson:"object-size"`

	// ChunkSize is an internal field indicating the maximal message payload size.
	// This field should not be set by users.
	ChunkSize int `json:"chunkSize" bson:"chunk-size"`
}

// ChunkInfo describes chunks for multi-inflight data transfer.
// swagger:ignore
type ChunkInfo struct {
	ResendTime int64 `json:"resendTime" bson:"resend-time"`
}

// Notification is used to store notifications in the store
// swagger:ignore
type Notification struct {
	ObjectID   string `json:"objectID" bson:"object-id"`
	ObjectType string `json:"objectType" bson:"object-type"`
	DestOrgID  string `json:"destinationOrgID" bson:"destination-org-id"`
	DestID     string `json:"destinationID" bson:"destination-id"`
	DestType   string `json:"destinationType" bson:"destination-type"`
	Status     string `json:"status" bson:"status"`
	InstanceID int64  `json:"instanceID" bson:"instance-id"`
	ResendTime int64  `json:"resendTime" bson:"resend-time"`
}

// StoreDestinationStatus is the information about destinations and their status for an object
// swagger:ignore
type StoreDestinationStatus struct {
	Destination Destination `bson:"destination"`
	Status      string      `bson:"status"`
}

// DestinationsStatus describes the delivery status of an object for a destination
// DestinationsStatus provides information about the delivery status of an object for a certain destination.
// The status can be one of the following:
// Indication whether the object has been delivered to the destination
//   pending - inidicates that the object is pending delivery to this destination
//   delivering - indicates that the object is being delivered to this destination
//   delivered - indicates that the object was delivered to this destination
// swagger:model
type DestinationsStatus struct {
	// DestType is the destination type
	//   required: true
	DestType string `json:"destinationType" bson:"destination-type"`

	// DestID is the destination ID
	//   required: true
	DestID string `json:"destinationID" bson:"destination-id"`

	// Status is the destination status
	//   required: true
	Status string `json:"status" bson:"status"`
}

// Organization contains organization's information
// swagger:model
type Organization struct {
	// OrgID is the organization ID of the organization
	OrgID string `json:"org-id" bson:"org-id"`

	// User is the user name to be used when connecting to this organization
	User string `json:"user" bson:"user"`

	// Password is the password to be used when connecting to this organization
	Password string `json:"password" bson:"password"`

	// Address is the broker address to be used when connecting to this organization
	Address string `json:"address" bson:"address"`
}

// StoredOrganization contains organization and its update timestamp
type StoredOrganization struct {
	Org       Organization
	Timestamp time.Time
}

// MessagingGroup maps organization to its messaging group
type MessagingGroup struct {
	OrgID     string
	GroupName string
}

// Object status
const (
	NotReadyToSend     = "notReady"           // The object is not ready to be sent to the other side
	ReadyToSend        = "ready"              // The object is ready to be sent to the other side
	PartiallyReceived  = "partiallyreceived"  // Received the object from the other side, waiting for its data
	CompletelyReceived = "completelyReceived" // The object was received completely from the other side
	ObjConsumed        = "objconsumed"        // The object was consumed by the app
	ObjDeleted         = "objdeleted"         // The object was deleted by the other side
	ObjReceived        = "objreceived"        // The object was received by the app
)

// Notification status and type
const (
	Update                = "update"
	Updated               = "updated"
	Consumed              = "consumed"
	AckConsumed           = "ackconsumed"
	Getdata               = "getdata"
	Data                  = "data"
	UpdatePending         = "updatePending"
	ConsumedPending       = "consumedPending"
	Delete                = "delete"
	DeletePending         = "deletePending"
	Deleted               = "deleted"
	DeletedPending        = "deletedPending"
	AckDelete             = "ackDelete"
	AckDeleted            = "ackDeleted"
	Resend                = "resend"
	AckResend             = "ackresend"
	Register              = "register"
	AckRegister           = "regack"
	Received              = "received"
	ReceivedPending       = "receivedpending"
	AckReceived           = "ackreceived"
	ReceivedByDestination = "receivedByDest"
)

// Indication whether the object has been delivered to the destination
const (
	Pending    = "pending"
	Delivering = "delivering"
	Delivered  = "delivered"
	// Consumed
)

// Magic is a magic number placed in the front of various payloads
const Magic = 0x01010101

// Registered indicates if this node, an ESS, has registered itself
var Registered bool

// ResendAcked indicates if the resend objects request had been acknowledged
var ResendAcked bool

// Running indicates that the Sync Service is running
var Running bool

// Version is the current version of the Sync-Service
const Version = 1

// SingleOrgCSS is true in case of CSS ouside WIoTP with one organization set in the configration,
// and false otherwise
var SingleOrgCSS bool

// HTTPCSSURL specifies the CSS URL for HTTP communication from ESS
var HTTPCSSURL string

// HashStrings uses FNV-1a (Fowler/Noll/Vo) fast and well dispersed hash functions
// Reference: http://www.isthe.com/chongo/tech/comp/fnv/index.html
const (
	fnv32Init  uint32 = 0x811c9dc5
	fnv32Prime uint32 = 0x01000193
)

// Types of various ACLs
const (
	DestinationsACLType = "destinations"
	ObjectsACLType      = "objects"
)

// HashStrings hashes strings
func HashStrings(strings ...string) uint32 {
	h := fnv32Init
	for _, s := range strings {
		l := len(s)
		for i := 0; i < l; i++ {
			h ^= uint32(s[i])
			h *= fnv32Prime
		}
	}
	return h
}

// GetNotificationID gets the notification ID for the notification
func GetNotificationID(notification Notification) string {
	return CreateNotificationID(notification.DestOrgID, notification.ObjectType, notification.ObjectID, notification.DestType,
		notification.DestID)
}

// CreateNotificationID creates notification ID
func CreateNotificationID(orgID string, objectType string, objectID string, destType string, destID string) string {
	var strBuilder strings.Builder
	strBuilder.Grow(len(orgID) + len(objectType) + len(objectID) + len(destType) + len(destID) + 5)
	strBuilder.WriteString(orgID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectType)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(objectID)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(destType)
	strBuilder.WriteByte(':')
	strBuilder.WriteString(destID)
	return strBuilder.String()
}
