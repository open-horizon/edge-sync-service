package security

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

var authenticator Authentication

type authenticationCacheElement struct {
	appSecret  string
	code       int
	orgID      string
	userid     string
	expiration time.Time
}

const authenticationCacheDuration = 15 * time.Minute

var authenticationCache map[string]authenticationCacheElement
var authenticationCacheLock sync.RWMutex

var cacheFlushTicker *time.Ticker
var cacheFlushStopChannel chan int

// Store is a reference to the storage in use
var Store storage.Storage

// Start starts up the security component
func Start() {
	authenticator.Start()

	authenticationCache = make(map[string]authenticationCacheElement)

	cacheFlushStopChannel = make(chan int, 1)
	cacheFlushTicker = time.NewTicker(2 * authenticationCacheDuration)
	go func() {
		keepRunning := true
		for keepRunning {
			select {
			case <-cacheFlushTicker.C:
				flushAuthenticationCache()

			case <-cacheFlushStopChannel:
				keepRunning = false
			}
		}
		cacheFlushTicker = nil
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
// user's org and identitity. An edge node's identity is destType/destID
func Authenticate(request *http.Request) (int, string, string) {
	appKey, appSecret, ok := request.BasicAuth()
	if !ok {
		return AuthFailed, "", ""
	}

	authenticationCacheLock.RLock()
	entry, ok := authenticationCache[appKey]
	authenticationCacheLock.RUnlock()

	now := time.Now()
	if ok && appSecret == entry.appSecret {
		if now.Before(entry.expiration) {
			return entry.code, entry.orgID, entry.userid
		}

	}
	code, orgID, userID := authenticator.Authenticate(request)
	if code != AuthFailed {
		entry = authenticationCacheElement{appSecret, code, orgID, userID, now.Add(authenticationCacheDuration)}
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
func CanUserCreateObject(request *http.Request, orgID string, metaData *common.MetaData) bool {
	code, userOrgID, userID := Authenticate(request)
	if code == AuthFailed || code == AuthEdgeNode || userOrgID != orgID {
		return false
	}

	if common.Configuration.NodeType == common.ESS {
		return true
	}

	if code == AuthAdmin {
		return true
	}

	if metaData.DestType == "" && 0 == len(metaData.DestinationsList) {
		// Only Admins can send out broadcasts
		return false
	}

	if !checkObjectAccessByUser(userID, orgID, metaData.ObjectType) {
		return false
	}

	destinationTypes := getDestinationTypes(metaData)
	for _, destinationType := range destinationTypes {
		usernames, err := Store.RetrieveACL(common.DestinationsACLType, orgID, destinationType)
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, destinationType, err)
			}
			return false
		}

		for _, username := range usernames {
			if username == "*" || username == userID {
				return true
			}
		}
	}
	return false
}

// CanUserAccessObject checks if the user identified by the credentials in the supplied request,
// can read/modify the specified object type.
func CanUserAccessObject(request *http.Request, orgID, objectType string) bool {
	code, userOrgID, userID := Authenticate(request)
	if code == AuthFailed || code == AuthEdgeNode || userOrgID != orgID {
		return false
	}

	if common.Configuration.NodeType == common.ESS {
		return true
	}

	if code == AuthAdmin {
		return true
	}

	return checkObjectAccessByUser(userID, orgID, objectType)
}

// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func KeyandSecretForURL(url string) (string, string) {
	return authenticator.KeyandSecretForURL(url)
}

func checkObjectAccessByUser(userID, orgID, objectType string) bool {
	usernames, err := Store.RetrieveACL(common.ObjectsACLType, orgID, objectType)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to fetch ACL for %s %s. Error: %s", orgID, objectType, err)
		}
		return false
	}

	for _, username := range usernames {
		if username == "*" || username == userID {
			return true
		}
	}
	return false
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
