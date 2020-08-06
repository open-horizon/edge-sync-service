package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

var authenticator Authentication

const saltLength = 24

type authenticationCacheElement struct {
	appSecret  []byte
	salt       []byte
	code       int
	orgID      string
	userid     string
	expiration time.Time
}

type destinationACLCacheElement struct {
	users      []common.ACLentry
	expiration time.Time
}

// SPIRequestIdentityHeader is the header used to send the identity in HTTP SPI requests
// Should only be used here and in the httpCommunication tests
const SPIRequestIdentityHeader = "X-Sync-Service-Dest"

var spiRequestIdentity string

const cacheDuration = 15 * time.Minute

var authenticationCache map[string]authenticationCacheElement
var authenticationCacheLock sync.RWMutex

var destinationACLCache map[string]destinationACLCacheElement
var destinationACLCacheLock sync.RWMutex

var cacheFlushTicker *time.Ticker
var cacheFlushStopChannel chan int

// Store is a reference to the storage in use
var Store storage.Storage

// Start starts up the security component
func Start() {
	authenticator.Start()

	if common.Configuration.NodeType == common.ESS {
		spiRequestIdentity = common.Configuration.OrgID + "/" +
			common.Configuration.DestinationType + "/" + common.Configuration.DestinationID
	}

	authenticationCache = make(map[string]authenticationCacheElement)
	destinationACLCache = make(map[string]destinationACLCacheElement)

	cacheFlushStopChannel = make(chan int, 1)
	cacheFlushTicker = time.NewTicker(2 * cacheDuration)
	go func() {
		common.GoRoutineStarted()
		keepRunning := true
		for keepRunning {
			select {
			case <-cacheFlushTicker.C:
				flushAuthenticationCache()
				flushDestinationACLCache()

			case <-cacheFlushStopChannel:
				keepRunning = false
			}
		}
		cacheFlushTicker = nil
		common.GoRoutineEnded()
	}()
}

// Stop stops the security component
func Stop() {
	if cacheFlushTicker != nil {
		cacheFlushTicker.Stop()
		cacheFlushStopChannel <- 1
	}
}

// SetAuthentication is called by the code starting the Sync Service to set the
// Authentication implementation to be used by the Sync Service.
func SetAuthentication(auth Authentication) {
	authenticator = auth
}

// Authenticate  authenticates a particular HTTP request and indicates
// whether it is an edge node, org admin, or plain user. Also returned is the
// user's org and identitity. An edge node's identity is destType/destID. A
// service's identity is serviceOrg/arch/version/serviceName.
func Authenticate(request *http.Request) (int, string, string) {
	appKey, appSecret, ok := request.BasicAuth()
	if !ok {
		return AuthFailed, "", ""
	}

	authenticationCacheLock.RLock()
	entry, ok := authenticationCache[appKey]
	authenticationCacheLock.RUnlock()

	now := time.Now()
	if ok && now.Before(entry.expiration) {
		secretMAC, _, err := saltSecret(appSecret, entry.salt)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("%s", err)
			}
			return AuthFailed, "", ""
		}

		if hmac.Equal(secretMAC, entry.appSecret) {
			return entry.code, entry.orgID, entry.userid
		}
	}

	code, orgID, userID := authenticator.Authenticate(request)
	if code != AuthFailed {
		secretMAC, salt, err := saltSecret(appSecret, nil)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("%s", err)
			}
			return AuthFailed, "", ""
		}

		entry = authenticationCacheElement{secretMAC, salt, code, orgID, userID, now.Add(cacheDuration)}
		authenticationCacheLock.Lock()
		authenticationCache[appKey] = entry
		authenticationCacheLock.Unlock()
	} else {
		authenticationCacheLock.Lock()
		delete(authenticationCache, appKey)
		authenticationCacheLock.Unlock()
	}

	return code, orgID, userID
}

// CanUserCreateObject checks if the user identified by the credentials in the supplied request,
// can create an object of the object type, and send it to the destinations in the meta data.
func CanUserCreateObject(request *http.Request, orgID string, metaData *common.MetaData) (bool, string, string) {
	code, userOrgID, userID := Authenticate(request)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.CanUserCreateObject: authcode is %d, userOrgID is %s, userID is %s", code, userOrgID, userID)
	}
	if code == AuthSyncAdmin {
		return true, userOrgID, userID
	}

	if code == AuthFailed || code == AuthEdgeNode || userOrgID != orgID {
		return false, userOrgID, userID
	}

	if common.Configuration.NodeType == common.ESS {
		return true, userOrgID, userID
	}

	if code == AuthAdmin {
		return true, userOrgID, userID
	}

	aclUserType := ""
	if code == AuthUser {
		aclUserType = ACLUser
	} else if code == AuthNodeUser {
		aclUserType = ACLNode
	}

	// check if given user has aclWriter access
	if !checkObjectCanBeModifiedByUser(userID, orgID, metaData.ObjectType, aclUserType) {
		if log.IsLogging(logger.ERROR) {
			log.Error("checkObjectCanBeModifiedByUser is false")
		}
		return false, userOrgID, userID
	}

	if metaData.DestType == "" && 0 == len(metaData.DestinationsList) && metaData.DestinationPolicy == nil {
		// Only Admins or user that has writer access can send out broadcasts
		if log.IsLogging(logger.ERROR) {
			log.Error("In security.CanUserCreateObject: given user %s with authcode %d cannot broadcast object, need to set either destinationType(destinationList) or destinationPolicy", userID, code)
		}
		return false, userOrgID, userID
	}

	if common.Configuration.NodeType == common.ESS {
		return true, userOrgID, userID
	}

	destinationTypes := getDestinationTypes(metaData)
	if trace.IsLogging(logger.INFO) {
		trace.Info("Starting to check if the given user has access to the destination(s) defined in metadata")
	}
	for _, destinationType := range destinationTypes {
		if !checkDestinationAccessByUser(userID, orgID, destinationType, aclUserType) {
			if log.IsLogging(logger.ERROR) {
				log.Error("Given userID %s (orgID: %s, aclUserType: %s) doesn't have access to destination type: %s", userID, orgID, aclUserType, destinationType)
			}
			return false, userOrgID, userID
		}
	}
	return true, userOrgID, userID
}

// CanUserAccessObject checks if the user identified by the credentials in the supplied request,
// can read/modify the specified object type.
func CanUserAccessObject(request *http.Request, orgID, objectType string) (int, string) {
	code, userOrgID, userID := Authenticate(request)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.CanUserAccessObject: authcode is %d, userOrgID is %s, userID is %s", code, userOrgID, userID)
	}
	if code == AuthSyncAdmin {
		return code, userID
	}

	if code == AuthFailed || code == AuthEdgeNode || userOrgID != orgID {
		return AuthFailed, ""
	}

	if common.Configuration.NodeType == common.ESS {
		return code, userID
	}

	if code == AuthAdmin {
		return code, userID
	}

	// CSS, code == authUser || AuthNodeUser
	aclUserType := ""
	if code == AuthUser {
		aclUserType = ACLUser
	} else if code == AuthNodeUser {
		aclUserType = ACLNode
	}

	if checkObjectAccessByUser(userID, orgID, objectType, aclUserType) {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In security.CanUserAccessObject: checkObjectAccessByUser returns authcode %d for user %s", code, userID)
		}
		return code, userID
	}
	return AuthFailed, ""
}

// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func KeyandSecretForURL(url string) (string, string) {
	return authenticator.KeyandSecretForURL(url)
}

// AddIdentityToSPIRequest Adds identity related stuff to SPI requests made by an ESS
func AddIdentityToSPIRequest(request *http.Request, requestURL string) {
	// dummyAuthenticate: username: "{orgId}/{destinationType}/{destinationId}", password: ""
	username, password := authenticator.KeyandSecretForURL(requestURL)
	request.SetBasicAuth(username, password)

	request.Header.Add(SPIRequestIdentityHeader, spiRequestIdentity)
}

// ValidateSPIRequestIdentity validates the identity sent in a SPI request by an ESS to a CSS
// Returns true if the identity is ok for a SPI request, along with the orgID, destType, and
// destID sent in the request.
func ValidateSPIRequestIdentity(request *http.Request) (bool, string, string, string) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.ValidateSPIRequestIdentity")
	}
	var orgID string
	var destType string
	var destID string

	identityParts := strings.Split(request.Header.Get(SPIRequestIdentityHeader), "/")
	if len(identityParts) != 3 {
		return false, "", "", ""
	}

	code, orgID, user := Authenticate(request)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.ValidateSPIRequestIdentity, code is %d, orgID is %s, user is %s", code, orgID, user)
	}
	switch code {
	case AuthEdgeNode:
		// user edge/node2
		parts := strings.Split(user, "/")
		if len(parts) != 2 {
			return false, "", "", ""
		}
		if orgID != identityParts[0] || parts[0] != identityParts[1] || parts[1] != identityParts[2] {
			return false, "", "", ""
		}
		destType = parts[0]
		destID = parts[1]

	case AuthAdmin:
		if orgID != identityParts[0] {
			return false, "", "", ""
		}
		destType = identityParts[1]
		destID = identityParts[2]

	case AuthUser:
		if checkDestinationAccessByUser(user, orgID, identityParts[1], "") {
			destType = identityParts[1]
			destID = identityParts[2]
		} else {
			return false, "", "", ""
		}

	default:
		return false, "", "", ""
	}
	return true, orgID, destType, destID
}

func saltSecret(appSecret string, salt []byte) ([]byte, []byte, error) {
	if salt == nil {
		salt = make([]byte, saltLength)
		_, err := rand.Read(salt)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to generate salt. Error: %s", err)
		}
	}
	secretHash := hmac.New(sha256.New, salt)
	_, err := secretHash.Write([]byte(appSecret))
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to generate HMAC. Error: %s", err)
	}

	return secretHash.Sum(nil), salt, nil
}

func checkObjectCanBeModifiedByUser(userID, orgID, objectType string, aclUserType string) bool {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.checkObjectCanBeModifiedByUser: userID is %s, orgID is %s, objectType is %s, aclUserType is %s", userID, orgID, objectType, aclUserType)
	}

	users, err := Store.RetrieveACL(common.ObjectsACLType, orgID, objectType, aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, objectType, err)
		}
		return false
	}

	for _, user := range users {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("ACL entry for objectType(%s): %s:%s:%s", objectType, user.ACLType, user.Username, user.ACLRole)
		}

		if user.Username == "*" || user.Username == userID {
			if user.ACLRole == ACLWriter {
				return true
			} else if user.ACLRole == ACLReader {
				return false
			}
		}

	}
	// Doesn't find username in acl list for given object type
	// then check if user is in acl list for all object type. (db entry with ID: {org}:objects)
	users, err = Store.RetrieveACL(common.ObjectsACLType, orgID, "", aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, objectType, err)
		}
		return false
	}
	for _, user := range users {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("ACL entry: %s:%s:%s", user.ACLType, user.Username, user.ACLRole)
		}

		if user.Username == "*" || user.Username == userID {
			if user.ACLRole == ACLWriter {
				return true
			} else if user.ACLRole == ACLReader {
				return false
			}
		}

	}
	return false
}

func checkObjectAccessByUser(userID, orgID, objectType string, aclUserType string) bool {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.checkObjectAccessByUser: userID is %s, orgID is %s, objectType is %s, aclUserType is %s", userID, orgID, objectType, aclUserType)
	}
	users, err := Store.RetrieveACL(common.ObjectsACLType, orgID, objectType, aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, objectType, err)
		}
		return false
	}

	for _, user := range users {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("ACL entry for objectType(%s): %s:%s:%s", objectType, user.ACLType, user.Username, user.ACLRole)
		}

		if user.Username == "*" || user.Username == userID {
			return true
		}

	}

	// check if user is in acl list for all object type. (db entry with ID: {org}:objects:*)
	users, err = Store.RetrieveACL(common.ObjectsACLType, orgID, "", aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, objectType, err)
		}
		return false
	}
	for _, user := range users {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("ACL entry: %s:%s:%s", user.ACLType, user.Username, user.ACLRole)
		}

		if user.Username == "*" || user.Username == userID {
			return true
		}

	}
	return false
}

func checkDestinationAccessByUser(userID, orgID, destType string, aclUserType string) bool {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.checkDestinationAccessByUser: userID is %s, orgID is %s, destinationType is %s, aclUserType is %s", userID, orgID, destType, aclUserType)
	}
	cacheKey := orgID + ":" + destType
	var users []common.ACLentry

	destinationACLCacheLock.RLock()
	entry, ok := destinationACLCache[cacheKey]
	destinationACLCacheLock.RUnlock()

	now := time.Now()
	var err error
	if ok && now.Before(entry.expiration) {
		users = entry.users
	} else {
		users, err = Store.RetrieveACL(common.DestinationsACLType, orgID, destType, aclUserType)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, destType, err)
			}
			return false
		}

		entry := destinationACLCacheElement{users, now.Add(cacheDuration)}
		destinationACLCacheLock.Lock()
		destinationACLCache[cacheKey] = entry
		destinationACLCacheLock.Unlock()
	}

	for _, user := range users {
		if user.Username == "*" || user.Username == userID {
			return true
		}

	}

	users, err = Store.RetrieveACL(common.DestinationsACLType, orgID, "", aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, destType, err)
		}
		return false
	}
	for _, user := range users {
		if user.Username == "*" || user.Username == userID {
			return true
		}

	}

	return false
}

/*
// Used when user trying to update data for an object (will check if the given user has writer access to object on the destinations defined in metadata)
func checkObjectOnDestinationCanBeModifiedByUser(userID, orgID, destType string, aclUserType string) bool {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.checkDestinationAccessByUser: userID is %s, orgID is %s, destinationType is %s, aclUserType is %s", userID, orgID, destType, aclUserType)
	}
	cacheKey := orgID + ":" + destType
	var users []common.ACLentry

	destinationACLCacheLock.RLock()
	entry, ok := destinationACLCache[cacheKey]
	destinationACLCacheLock.RUnlock()

	now := time.Now()
	var err error
	if ok && now.Before(entry.expiration) {
		users = entry.users
	} else {
		// user:username1:{aclType}
		users, err = Store.RetrieveACL(common.DestinationsACLType, orgID, destType, aclUserType)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, destType, err)
			}
			return false
		}

		entry := destinationACLCacheElement{users, now.Add(cacheDuration)}
		destinationACLCacheLock.Lock()
		destinationACLCache[cacheKey] = entry
		destinationACLCacheLock.Unlock()
	}

	for _, user := range users {
		if user.Username == "*" || user.Username == userID {
			if user.ACLRole == ACLWriter {
				return true
			} else if user.ACLRole == ACLReader {
				return false
			}
		}

	}

	users, err = Store.RetrieveACL(common.DestinationsACLType, orgID, "", aclUserType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, destType, err)
		}
		return false
	}
	for _, user := range users {
		if user.Username == "*" || user.Username == userID {
			if user.ACLRole == ACLWriter {
				return true
			} else if user.ACLRole == ACLReader {
				return false
			}
		}

	}

	return false
} */

// CheckAddACLInputFormat checks ACL entry format. Should be in the format of {usertype}:{username/nodename}:{aclrole}
func CheckAddACLInputFormat(aclType string, aclInputList []common.ACLentry) (*[]common.ACLentry, error) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.ChecAddkACLInputFormat")
	}

	if len(aclInputList) == 0 {
		return nil, nil
	}

	var message string
	var updatedAclList []common.ACLentry
	if aclType == "destinations" {
		updatedAclList = make([]common.ACLentry, 0)
	}
	for _, aclInput := range aclInputList {

		aclUserType := aclInput.ACLType
		name := aclInput.Username

		if aclUserType != ACLUser && aclUserType != ACLNode {
			message = fmt.Sprintf("aclUserType \"%s\" is invalid for ACL entry %s, it should be \"%s\", or \"%s\"", aclUserType, aclInput, ACLUser, ACLNode)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			return nil, errors.New(message)
		}

		//trimName := strings.TrimSpace(name)
		if strings.TrimSpace(name) == "" {
			message = fmt.Sprintf("username/nodename cannot be empty for ACL entry %s.", aclInput)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			return nil, errors.New(message)
		}

		if aclType == "objects" {
			role := aclInput.ACLRole
			if role != ACLWriter && role != ACLReader {
				message = fmt.Sprintf("aclRole \"%s\" is invalid for ACL entry %s, it should be \"%s\", or \"%s\"", role, aclInput, ACLWriter, ACLReader)
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
				return nil, errors.New(message)
			}
		} else {
			// aclType == "destinations", there is not role for destinations acl entry, set role to "N/A"
			aclInput.ACLRole = ACLNA
			updatedAclList = append(updatedAclList, aclInput)
			fmt.Printf("updated aclUserInput: %s:%s:%s", aclInput.ACLType, aclInput.Username, aclInput.ACLRole)
		}

	}

	if aclType == "objects" {
		return nil, nil
	}
	// aclType == "destinations", return the updated aclInputList
	return &updatedAclList, nil
}

// CheckRemoveACLInputFormat checks ACL entry format. Should be in the format of {usertype}:{username/nodename}
func CheckRemoveACLInputFormat(aclInputList []common.ACLentry) error {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In security.CheckRemoveACLInputFormat")
	}

	if len(aclInputList) == 0 {
		return nil
	}

	var message string
	for _, aclInput := range aclInputList {
		aclUserType := aclInput.ACLType
		name := aclInput.Username

		if aclUserType != ACLUser && aclUserType != ACLNode {
			message = fmt.Sprintf("aclUserType \"%s\" is invalid for ACL entry %s, it should be \"%s\", or \"%s\"", aclUserType, aclInput, ACLUser, ACLNode)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			return errors.New(message)
		}

		//trimName := strings.TrimSpace(name)
		if strings.TrimSpace(name) == "" {
			message = fmt.Sprintf("username/nodename cannot be empty for ACL entry %s.", aclInput)
			if log.IsLogging(logger.ERROR) {
				log.Error(message)
			}
			return errors.New(message)
		}

	}
	return nil
}

func getDestinationTypes(metaData *common.MetaData) []string {
	result := make([]string, 0)

	if metaData.DestType != "" {
		result = append(result, metaData.DestType)
	}

	for _, fullDestination := range metaData.DestinationsList {
		parts := strings.Split(fullDestination, ":")
		destType := strings.TrimSpace(parts[0])

		notFound := true
		for _, resultEntry := range result {
			if resultEntry == destType {
				notFound = false
				break
			}
		}
		if notFound {
			result = append(result, destType)
		}
	}

	return result
}

func flushAuthenticationCache() {
	now := time.Now()

	authenticationCacheLock.Lock()
	defer authenticationCacheLock.Unlock()

	for cacheKey, entry := range authenticationCache {
		if now.After(entry.expiration) {
			delete(authenticationCache, cacheKey)
		}
	}
}

func flushDestinationACLCache() {
	now := time.Now()

	destinationACLCacheLock.Lock()
	defer destinationACLCacheLock.Unlock()

	for cacheKey, entry := range destinationACLCache {
		if now.After(entry.expiration) {
			delete(destinationACLCache, cacheKey)
		}
	}
}
