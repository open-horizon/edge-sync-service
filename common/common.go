package common

import (
	"crypto"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
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

// IsInvalidRequest returns true if the error passed in is the common.InvalidRequest error
func IsInvalidRequest(err error) bool {
	_, ok := err.(*InvalidRequest)
	return ok
}

// SetupError is the error for setup issues
// swagger:ignore
type SetupError struct {
	Message string
}

func (e *SetupError) Error() string {
	return e.Message
}

// SecurityError is the error for requests failed because of security
type SecurityError struct {
	Message string
}

func (e *SecurityError) Error() string {
	return e.Message
}

// IOError is the error for requests failed because of the IO
type IOError struct {
	Message string
}

func (e *IOError) Error() string {
	return e.Message
}

// PathError is the error for path issues
type PathError struct {
	Message string
}

func (e *PathError) Error() string {
	return e.Message
}

// InternalError is a general error
type InternalError struct {
	Message string
}

func (e *InternalError) Error() string {
	return e.Message
}

// NotFound is the error returned if an object wasn't found
type NotFound struct {
	message string
}

func (e *NotFound) Error() string {
	if e.message == "" {
		return "Object was not found"
	}
	return e.message
}

// IsNotFound returns true if the error passed in is the common.NotFound error
func IsNotFound(err error) bool {
	_, ok := err.(*NotFound)
	return ok
}

// InvalidRequest is the error for invalid reguests
// swagger:ignore
type IgnoredRequest struct {
	Message string
}

func (e *IgnoredRequest) Error() string {
	return e.Message
}

// IsInvalidRequest returns true if the error passed in is the common.InvalidRequest error
func IsIgnoredRequest(err error) bool {
	_, ok := err.(*IgnoredRequest)
	return ok
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
	//   enum:  HTTP,MQTT
	Communication string `json:"communication" bson:"communication"`

	// CodeVersion is the sync service code version used by the destination
	//   required: true
	CodeVersion string `json:"codeVersion" bson:"code-version"`
}

// PolicyProperty is a property in a policy
// swagger:model
type PolicyProperty struct {
	// Name is the name of the property
	//   required: true
	Name string `json:"name" bson:"name"`

	// Value is the value of the property
	//   required: true
	Value interface{} `json:"value" bson:"value"`

	// Type is the type of the property.
	// It is only required where the system can't interpret the value correctly by context.
	//   required: false
	Type string `json:"type" bson:"type"`
}

// ServiceID contains the ID of a service to which an object may have affinity for
// swagger:model
type ServiceID struct {
	// OrgID is the organization ID of the service
	OrgID string `json:"orgID" bson:"org-id"`

	// Arch is the architecture of the service
	Arch string `json:"arch" bson:"arch"`

	// ServiceName is the name of the service
	ServiceName string `json:"serviceName" bson:"service-name"`

	// Version is the version of the service
	Version string `json:"version" bson:"version"`
}

// Policy describes a policy made up of a set of properties and constraints
// swagger:model
type Policy struct {
	// Properties is the set of properties for a particular policy
	Properties []PolicyProperty `json:"properties" bson:"properties"`

	// Constraints is a set of expressions that form the constraints for the policy
	Constraints []string `json:"constraints" bson:"constraints"`

	// Services is the list of services this object has affinity for
	Services []ServiceID `json:"services" bson:"services"`

	// Timestamp indicates when the policy was last updated (result of time.Now().UTC().UnixNano())
	Timestamp int64 `json:"timestamp" bson:"timestamp"`
}

// GetRemovedPolicyServices is the method to compare existing destination policy and new destination policy, returning removed policy services
func GetRemovedPolicyServices(existingPolicy *Policy, newPolicy *Policy) []ServiceID {
	removedServices := make([]ServiceID, 0)
	if existingPolicy == nil {
		return removedServices
	} else if newPolicy == nil { // existingPolicy != nil && newPolicy == nil, add all service from existingPolicy to returned list
		return existingPolicy.Services
	} else { // existingPolicy != nil && newPolicy != nil
		for _, existingService := range existingPolicy.Services {
			found := false
			for _, newService := range newPolicy.Services {
				if newService.OrgID == existingService.OrgID &&
					newService.ServiceName == existingService.ServiceName &&
					newService.Version == existingService.Version {
					found = true
					break

				}
			}

			if !found {
				removedServices = append(removedServices, existingService)
			}
		}
		return removedServices
	}
}

// ServiceListContains returns true if serviceIDList contains the given serviceID
func ServiceListContains(serviceList []ServiceID, service ServiceID) bool {
	for _, s := range serviceList {
		if s.OrgID == service.OrgID && s.ServiceName == service.ServiceName && s.Version == service.Version {
			return true
		}
	}
	return false
}

// RemoveServiceFromServiceList removes a service from the given service list and return it. Returns false if nothing removed from the list
func RemoveServiceFromServiceList(serviceID string, serviceList []ServiceID) ([]ServiceID, bool) {
	// serviceOrgID/version/serviceName
	parts := strings.SplitN(serviceID, "/", 3)
	if len(parts) < 3 {
		return serviceList, false
	}

	for i, s := range serviceList {
		if parts[0] == s.OrgID && parts[2] == s.ServiceName {
			if policySemVerRange, err := ParseSemVerRange(s.Version); err == nil {
				if serviceSemVer, err := ParseSemVer(parts[1]); err == nil {
					if policySemVerRange.IsInRange(serviceSemVer) {
						// find serviceList[i], remove it
						serviceList[i] = serviceList[len(serviceList)-1]
						return serviceList[:len(serviceList)-1], true
					}
				}
			}
		}
	}

	return serviceList, false
}

// StringListContains returns true if string list contains given string
func StringListContains(stringList []string, str string) bool {
	for _, s := range stringList {
		if s == str {
			return true
		}
	}
	return false
}

// GetHash returns the hash object and crypto hash algo name (used by rsa.VerifyPSS() func) of specified hash algorithm
func GetHash(hashAlgo string) (hash.Hash, crypto.Hash, SyncServiceError) {
	if hashAlgo == Sha1 {
		return sha1.New(), crypto.SHA1, nil
	} else if hashAlgo == Sha256 {
		return sha256.New(), crypto.SHA256, nil
	} else {
		return nil, 0, &InternalError{Message: fmt.Sprintf("Hash algorithm %s is not supported", hashAlgo)}
	}
}

func GetStartAndEndRangeFromRangeHeader(request *http.Request) (int64, int64, SyncServiceError) {
	// Get range from the "Range:bytes={startOffset}-{endOffset}"
	requestRangeAll := request.Header.Get("Range")
	if requestRangeAll == "" {
		return -1, -1, nil
	}
	requestRange := requestRangeAll[6:]
	ranges := strings.Split(requestRange, "-")

	if len(ranges) != 2 {
		return -1, -1, &InvalidRequest{Message: "Failed to parse Range header: " + requestRangeAll}
	}

	beginOffset, err := strconv.ParseInt(ranges[0], 10, 64)
	if err != nil {
		return -1, -1, &InvalidRequest{Message: "Failed to get begin offset from Range header: " + err.Error()}
	}

	endOffset, err := strconv.ParseInt(ranges[1], 10, 64)
	if err != nil {
		return -1, -1, &InvalidRequest{Message: "Failed to get end offset from Range header: " + err.Error()}
	}

	if beginOffset > endOffset {
		return -1, -1, &InvalidRequest{Message: "Begin offset cannot be greater than end offset"}
	}

	return beginOffset, endOffset, nil
}

// Content-Range: bytes 1-2/*\
// Returns totalsize, startOffset, endOffset, err
func GetStartAndEndRangeFromContentRangeHeader(request *http.Request) (int64, int64, int64, SyncServiceError) {
	// Get range from the "Range:bytes={startOffset}-{endOffset}"
	requestContentRange := request.Header.Get("Content-Range")
	if requestContentRange == "" {
		return 0, -1, -1, nil
	}
	contentRange := strings.Replace(requestContentRange, "bytes ", "", -1)
	// 1-2/30
	ranges := strings.Split(contentRange, "/")

	if len(ranges) != 2 {
		return 0, -1, -1, &InvalidRequest{Message: "Failed to parse Content-Range header: " + requestContentRange}
	}
	// [1-2, 30]
	totalSize, err := strconv.ParseInt(ranges[1], 10, 64)
	if err != nil {
		return 0, -1, -1, &InvalidRequest{Message: "Failed to get total size from Content-Range header: " + err.Error()}
	}

	offsets := strings.Split(ranges[0], "-")
	if len(offsets) != 2 {
		return 0, -1, -1, &InvalidRequest{Message: "Failed to get offsets from Content-Range header: " + requestContentRange}
	}

	startOffset, err := strconv.ParseInt(offsets[0], 10, 64)
	if err != nil {
		return 0, -1, -1, &InvalidRequest{Message: "Failed to get start offset from Content-Range header: " + err.Error()}
	}

	endOffset, err := strconv.ParseInt(offsets[1], 10, 64)
	if err != nil {
		return 0, -1, -1, &InvalidRequest{Message: "Failed to get end offset from Content-Range header: " + err.Error()}
	}

	if startOffset > endOffset {
		return 0, -1, -1, &InvalidRequest{Message: "Begin offset cannot be greater than end offset"}
	}

	return totalSize, startOffset, endOffset, nil
}

func GetMMSUploadOwnerHeader(request *http.Request) string {
	uploadOwner := request.Header.Get("MMS-Upload-Owner")
	return uploadOwner
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

	// DestinationPolicy is the policy specification that should be used to distribute this object
	// to the appropriate set of destinations.
	// When a DestinationPolicy is provided DestinationsList, DestType, and DestID must be omitted.
	DestinationPolicy *Policy `json:"destinationPolicy" bson:"destination-policy"`

	// Expiration is a timestamp/date indicating when the object expires.
	// When the object expires it is automatically deleted.
	// The timestamp should be provided in RFC3339 format.
	// This field is available only when working with the CSS.
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

	// OriginType is the type of origin of the object. Set by the internal code.
	// Read only field, should not be set by users.
	OriginType string `json:"originType" bson:"origin-type"`

	// Deleted is a flag indicating to applications polling for updates that this object has been deleted.
	// Read only field, should not be set by users.
	Deleted bool `json:"deleted" bson:"deleted"`

	// InstanceID is an internal instance ID.
	// This field should not be set by users.
	InstanceID int64 `json:"instanceID" bson:"instance-id"`

	// DataID is an internal data ID.
	// This field should not be set by users.
	DataID int64 `json:"dataID" bson:"data-id"`

	// ObjectSize is an internal field indicating the size of the object's data.
	// This field should not be set by users.
	ObjectSize int64 `json:"objectSize" bson:"object-size"`

	// ChunkSize is an internal field indicating the maximal message payload size.
	// This field should not be set by users.
	ChunkSize int `json:"chunkSize" bson:"chunk-size"`

	// HashAlgorithm used for data signature sign/verification. "SHA1" and "SHA256" are supported hash algorithms.
	// Valid values are: "SHA1", "SHA256"
	// Optional field, if omitted the data signature/verification will not be applied
	HashAlgorithm string `json:"hashAlgorithm" bson:"hash-algorithm"`

	// PublicKey is a base64 encoded string. It is the publicKey to verify the data of the object
	// Optional field, if omitted the data will not be verified with public key and signature
	PublicKey string `json:"publicKey" bson:"public-key"`

	// Signature is a base64 encoded string. It is the data signature to verify data of the object
	// Optional field, if omitted the data will not be verified with public key and signature
	Signature string `json:"signature" bson:"signature"`

	// Public is a flag indicate this object is visiable to all users in all orgs
	// Optional field, default is false (not visiable to all users)
	Public bool `json:"public" bson:"public"`

	// DataVerified is an internal field set by ESS after ESS downloads data from CSS or by CSS after ESS uploads data
	// Data can be obtained only when DataVerified field is true
	DataVerified bool `json:"dataVerified" bson:"data-verified"`

	// OwnerID is an internal field indicating who creates the object
	// This field should not be set by users
	OwnerID string `json:"ownerID" bson:"owner-id"`
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
	DataID     int64  `json:"dataID" bson:"data-id"`
	ResendTime int64  `json:"resendTime" bson:"resend-time"`
}

// StoreDestinationStatus is the information about destinations and their status for an object
// swagger:ignore
type StoreDestinationStatus struct {
	Destination Destination `bson:"destination"`
	Status      string      `bson:"status"`
	Message     string      `bson:"message"`
}

// DestinationsStatus describes the delivery status of an object for a destination
// DestinationsStatus provides information about the delivery status of an object for a certain destination.
// The status can be one of the following:
// Indication whether the object has been delivered to the destination
//   pending - inidicates that the object is pending delivery to this destination
//   delivering - indicates that the object is being delivered to this destination
//   delivered - indicates that the object was delivered to this destination
//   consumed - indicates that the object was consumed by this destination
//   deleted - indicates that this destination acknowledged the deletion of the object
//   error - indicates that a feedback error message was received from this destination
// swagger:model
type DestinationsStatus struct {
	// DestType is the destination type
	//   required: true
	DestType string `json:"destinationType"`

	// DestID is the destination ID
	//   required: true
	DestID string `json:"destinationID"`

	// Status is the destination status
	//   required: true
	//   enum: pending,delivering,delivered,consumed,deleted,error
	Status string `json:"status"`

	// Message is the message for the destination
	//    required: false
	Message string `json:"message"`
}

// ObjectStatus describes the delivery status of an object for a destination
// The status can be one of the following:
// Indication whether the object has been delivered to the destination
//   delivering - indicates that the object is being delivered
//   delivered - indicates that the object was delivered
//   consumed - indicates that the object was consumed
//   deleted - indicates that this destination acknowledged the deletion of the object
//   error - indicates that a feedback error message was received
// swagger:model
type ObjectStatus struct {
	// OrgID is the organization ID of the organization
	OrgID string `json:"orgID"`

	// ObjectType is the object type
	//   required: true
	ObjectType string `json:"objectType"`

	// ObjectID is the object ID
	//   required: true
	ObjectID string `json:"objectID"`

	// Status is the object status for this destination
	//   required: true
	//   enum: delivering,delivered,consumed,deleted,error
	Status string `json:"status"`
}

// ObjectDestinationPolicy contains information about an object that has a Destination Policy.
// swagger:model
type ObjectDestinationPolicy struct {
	// OrgID is the organization ID of the object (an object belongs to exactly one organization).
	//   required: true
	OrgID string `json:"orgID"`

	// ObjectType is the type of the object.
	// The type is used to group multiple objects, for example when checking for object updates.
	//   required: true
	ObjectType string `json:"objectType"`

	// ObjectID is a unique identifier of the object
	//   required: true
	ObjectID string `json:"objectID"`

	// DestinationPolicy is the policy specification that should be used to distribute this object
	// to the appropriate set of destinations.
	DestinationPolicy *Policy `json:"destinationPolicy,omitempty"`

	// Destinations is the list of the object's current destinations
	Destinations []DestinationsStatus `json:"destinations"`
}

// Organization contains organization's information
// swagger:model
type Organization struct {
	// OrgID is the organization ID of the organization
	OrgID string `json:"orgID" bson:"org-id"`

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

// ConsumedObject contains consumed object's meta data and its timestamp
type ConsumedObject struct {
	MetaData  MetaData
	Timestamp time.Time
}

// NotificationInfo contains information about a message to send to the other side
type NotificationInfo struct {
	NotificationTopic string
	DestType          string
	DestID            string
	InstanceID        int64
	DataID            int64
	MetaData          *MetaData
}

type ObjectInQueue struct {
	NotificationAction string // Notification status and type
	NotificationType   string
	Object             MetaData
	Destinations       []StoreDestinationStatus //use this list if NotificationType is common.TypeDestination
}

type DestinationRequestInQueue struct {
	Action      string
	Status      string
	Object      MetaData
	Destination Destination
}

// ACLentry contains ACL information about each user
type ACLentry struct {
	Username    string
	ACLUserType string
	ACLRole     string
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
	ConsumedByDest     = "consumedByDest"     // The object was consumed by the other side (ESS only)
)

// Notification status and type
const (
	Update                = "update"
	Updated               = "updated"
	HandleUpdate          = "handleUpdate"
	Consumed              = "consumed"
	AckConsumed           = "ackconsumed"
	ConsumedByDestination = "consumedByDest"
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
	RegisterNew           = "registerNew"
	RegisterAsNew         = "registerAsNew"
	Unregister            = "unregister"
	Received              = "received"
	ReceivedPending       = "receivedpending"
	AckReceived           = "ackreceived"
	ReceivedByDestination = "receivedByDest"
	Feedback              = "feedback"
	Error                 = "error"
	Ping                  = "ping"
	ReceiverError         = "receiverError"
)

// Indication whether the object has been delivered to the destination
const (
	Pending    = "pending"
	Delivering = "delivering"
	Delivered  = "delivered"
	// Consumed (defined above)
	// Error (defined above)
	// Deleted (defined above)
)

// Feedback codes
const (
	InternalErrorCode = 1
	IOErrorCode       = 2
	SecurityErrorCode = 3
	PathErrorCode     = 4
	InvalidObject     = 5

	// All error codes must have a value below this value
	// and all feedback codes must have a value above this value
	lastErrorCode = 10000
)

// Magic is a magic number placed in the front of various payloads
const Magic = uint32(0x01010101)

// Registered indicates if this node, an ESS, has registered itself
var Registered bool

// ResendAcked indicates if the resend objects request had been acknowledged
var ResendAcked bool

// Running indicates that the Sync Service is running
var Running bool

// SyncServiceVersion is the current version of the Sync-Service
type SyncServiceVersion struct {
	Major uint32
	Minor uint32
}

// Version is the current version of the Sync-Service
var Version SyncServiceVersion

// VersionAsString returns the current version as string
func VersionAsString() string {
	return fmt.Sprintf("%d.%d", Version.Major, Version.Minor)
}

// SingleOrgCSS is true in case of CSS ouside WIoTP with one organization set in the configration,
// and false otherwise
var SingleOrgCSS bool

// HTTPCSSURL specifies the CSS URL for HTTP communication from ESS
var HTTPCSSURL string

// ServingAPIs when true, indicates that the Sync Service is serving the various APIs over HTTP
var ServingAPIs bool

// Types of various ACLs
const (
	DestinationsACLType = "destinations"
	ObjectsACLType      = "objects"
)

// API payload actions
const (
	AddAction      = "add"
	RemoveAction   = "remove"
	RegisterAction = "register"
	DeleteAction   = "delete"
)

// DestinationUpdateRequestInQueue Action
const (
// Update = "update"
)

// NotificationType of object sent to objectWorkQueue
const (
	TypeDestination = "destination"
	TypeObject      = "object"
)

// Resend flag options
const (
	ResendAll = iota
	ResendDelivered
	ResendUndelivered
)

// Storage providers
const (
	Bolt     = "bolt"
	InMemory = "inmemory"
	Mongo    = "mongo"
)

// HashStrings uses FNV-1a (Fowler/Noll/Vo) fast and well dispersed hash functions
// Reference: http://www.isthe.com/chongo/tech/comp/fnv/index.html
const (
	fnv32Init  uint32 = 0x811c9dc5
	fnv32Prime uint32 = 0x01000193
)

// Hash Algorithms supported for digital signature
const (
	Sha1   = "SHA1"
	Sha256 = "SHA256"
)

func init() {
	ServingAPIs = true
}

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

// Locks is a set of object locks
type Locks struct {
	numberOfLocks uint32
	locks         []sync.RWMutex
	name          string
}

// NewLocks initializes object locks
func NewLocks(name string) *Locks {
	locks := Locks{name: name}
	if Configuration.NodeType == ESS {
		locks.numberOfLocks = 256
	} else {
		locks.numberOfLocks = 1024
	}

	locks.locks = make([]sync.RWMutex, locks.numberOfLocks)
	return &locks
}

// ObjectDownloadSemaphore sets the concurrent spi object download concurrency
var ObjectDownloadSemaphore *semaphore.Weighted

// InitObjectDownloadSemaphore initializes ObjectDownloadSemaphore
func InitObjectDownloadSemaphore() {
	maxWorkers := runtime.GOMAXPROCS(-1) * Configuration.HTTPCSSObjDownloadConcurrencyMultiplier
	ObjectDownloadSemaphore = semaphore.NewWeighted(int64(maxWorkers))
}

// ObjectLocks are locks for object and notification changes
var ObjectLocks Locks

// InitObjectLocks initializes ObjectLocks
func InitObjectLocks() {
	ObjectLocks = *NewLocks("object")
}

// Lock locks the object
func (locks *Locks) Lock(index uint32) {
	locks.locks[index&(locks.numberOfLocks-1)].Lock()
}

// Unlock unlocks the object
func (locks *Locks) Unlock(index uint32) {
	locks.locks[index&(locks.numberOfLocks-1)].Unlock()
}

// RLock locks the object for reading
func (locks *Locks) RLock(index uint32) {
	locks.locks[index&(locks.numberOfLocks-1)].RLock()
}

// RUnlock unlocks the object for reading
func (locks *Locks) RUnlock(index uint32) {
	locks.locks[index&(locks.numberOfLocks-1)].RUnlock()
}

// ConditionalLock locks the object if the index doesn't correspond to a lock that is already taken
func (locks *Locks) ConditionalLock(index uint32, lockedIndex uint32) {
	if index&(locks.numberOfLocks-1) != lockedIndex&(locks.numberOfLocks-1) {
		locks.locks[index&(locks.numberOfLocks-1)].Lock()
	}
}

// ConditionalUnlock unlocks the object if the index doesn't correspond to a lock that is already taken
func (locks *Locks) ConditionalUnlock(index uint32, lockedIndex uint32) {
	if index&(locks.numberOfLocks-1) != lockedIndex&(locks.numberOfLocks-1) {
		locks.locks[index&(locks.numberOfLocks-1)].Unlock()
	}
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

// CreateFeedback extracts feedback parameters from an error
func CreateFeedback(err SyncServiceError) (code int, retryInterval int32, reason string) {
	retryInterval = 0
	reason = err.Error()
	switch err.(type) {
	case *SecurityError:
		code = SecurityErrorCode
	case *IOError:
		code = IOErrorCode
	case *PathError:
		code = PathErrorCode
	case *NotFound:
		code = InvalidObject
	default:
		code = InternalErrorCode
	}
	return
}

// IsErrorFeedback returns true if the feedback code corresponds to an error
func IsErrorFeedback(code int) bool {
	if code < lastErrorCode {
		return true
	}
	return false
}

// CreateError creates a sync-service error from a Go error
func CreateError(err error, message string) SyncServiceError {
	if os.IsPermission(err) {
		return &SecurityError{message + err.Error()}
	}
	switch err.(type) {
	case *os.PathError:
		return &PathError{message + err.Error()}
	case *os.LinkError:
		return &IOError{message + err.Error()}
	case *os.SyscallError:
		return &IOError{message + err.Error()}
	default:
		return &InternalError{message + err.Error()}
	}
}

var goRoutinesCounter int
var routinesLock sync.RWMutex
var waitingOnBlockChannel bool
var blockChannel chan int

// ResetGoRoutineCounter sets the go routines counter to 0
func ResetGoRoutineCounter() {
	routinesLock.Lock()
	goRoutinesCounter = 0
	routinesLock.Unlock()
	blockChannel = make(chan int, 1)
}

// GoRoutineStarted increments the go routines counter
func GoRoutineStarted() {
	routinesLock.Lock()
	goRoutinesCounter++
	routinesLock.Unlock()
}

// GoRoutineEnded decrements the go routines counter
func GoRoutineEnded() {
	routinesLock.Lock()
	goRoutinesCounter--
	if waitingOnBlockChannel && goRoutinesCounter <= 0 {
		blockChannel <- 1
	}
	routinesLock.Unlock()
}

// BlockUntilNoRunningGoRoutines blocks the current "thread"
func BlockUntilNoRunningGoRoutines() {
	routinesLock.RLock()
	if goRoutinesCounter <= 0 {
		routinesLock.RUnlock()
		return
	}

	waitingOnBlockChannel = true
	routinesLock.RUnlock()

	timer := time.NewTimer(time.Duration(Configuration.ShutdownQuiesceTime) * time.Second)
	select {
	case <-blockChannel:
	case <-timer.C:
	}
	timer.Stop()

	waitingOnBlockChannel = false
}

func IsValidHashAlgorithm(hashAlgorithm string) bool {
	if hashAlgorithm == Sha1 || hashAlgorithm == Sha256 {
		return true
	}
	return false
}

func NeedDataVerification(metaData MetaData) bool {
	if IsValidHashAlgorithm(metaData.HashAlgorithm) && metaData.PublicKey != "" && metaData.Signature != "" {
		return true
	}
	return false
}

// IsValidName checks if the string only contains letters, digits, and !@#%^*-_.~
var IsValidName = regexp.MustCompile(`^[a-zA-Z0-9|!|@|#|$|^|*|\-|_|.|~|\pL|\pN]+$`).MatchString

// IsInvalidDescription checks if metadata description text contains XSS tags in this format: <abc> or </abc>
var IsInvalidDescription = regexp.MustCompile(`<[/]?[a-zA-Z0-9]+>`).MatchString

// ValidateDestinationListInput checks if destinationsList contains < or >, to avoid injecting html like tags from user
func ValidateDestinationListInput(destinationsList []string) (bool, SyncServiceError) {
	if len(destinationsList) == 0 {
		return true, nil
	}

	for _, destination := range destinationsList {
		if strings.ContainsAny(destination, "<") || strings.ContainsAny(destination, ">") {
			message := fmt.Sprintf("destinationsList contains unsupported char: < or > (%+v)", destination)
			return false, &InvalidRequest{Message: message}
		}
	}
	return true, nil
}

func init() {
	Version.Major = 1
	Version.Minor = 0
}
