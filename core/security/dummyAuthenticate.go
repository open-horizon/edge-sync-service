package security

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

// DummyAuthenticate is the dummy implementation of the Authenticate interface.
// It should NOT be used in production deployments.
//
// This implementation ignores App secrets.
//
// App keys for:
//
//     APIs        are of the form userID@orgID or email@emailDomain@orgID.
//                 The file {PersistentRootPath}/sync/dummy-auth.json is used to
//                 determine if a userID is a regular user or a sync admin.
//
//                 The file {PersistentRootPath}/sync/dummy-auth.json is of the form:
//                    {
//                      "regularUsers": [ "user1", "user2" ],
//                      "syncAdmins": [ "admin" ]
//                    }
//                 The userIDs in the field regularUsers are regular users and the
//                 userIDs in the field syncAdmins are sync-service administrators.
//
//                 If a userID does not appear in the file, it is assumed to be an
//                 admin for the specified org.
//
//     Edge nodes  are of the form orgID/destType/destID
type DummyAuthenticate struct {
	regularUsers []string
	syncAdmins   []string
}

const dummyAuthFilename = "/sync/dummy-auth.json"

type authInfo struct {
	RegularUsers []string `json:"regularUsers"`
	SyncAdmins   []string `json:"syncAdmins"`
}

// Start initializes the DummyAuthenticate struct
func (auth *DummyAuthenticate) Start() {
	authFile, err := os.Open(common.Configuration.PersistenceRootPath + dummyAuthFilename)
	if err != nil {
		if log.IsLogging(logger.WARNING) {
			if os.IsNotExist(err) {
				log.Warning("dummy-auth.json file not found. All users will be treated as org admins.")
			} else {
				log.Warning("Failed to open dummy-auth.json file. All users will be treated as org admins.\n Error: %s\n", err)
			}
		}
		auth.regularUsers = make([]string, 0)
		auth.syncAdmins = make([]string, 0)
		return
	}
	decoder := json.NewDecoder(authFile)
	var info authInfo
	err = decoder.Decode(&info)
	if err == nil {
		auth.regularUsers = info.RegularUsers
		auth.syncAdmins = info.SyncAdmins
	} else {
		auth.regularUsers = make([]string, 0)
		auth.syncAdmins = make([]string, 0)
	}

	return
}

// Authenticate  authenticates a particular HTTP request and indicates
// whether it is an edge node, org admin, or plain user. Also returned is the
// user's org and identitity. An edge node's identity is destType/destID. A
// service's identity is serviceOrg/version/serviceName.
//
// Note: This Authenticate implementation is for development use. App secrets
//      are ignored. App keys for APIs are of the form, userID@orgID or
//      email@emailDomain@orgID. The file dummy-auth.json is used to determine
//      if a userID is a regular user or a sync admin. If the userID does not
//      appear there, it is assumed to be an admin for the specified org.
//      Edge node app keys are of the form orgID/destType/destID
func (auth *DummyAuthenticate) Authenticate(request *http.Request) (int, string, string) {
	appKey, _, ok := request.BasicAuth()
	if !ok {
		return AuthFailed, "", ""
	}

	parts := strings.Split(appKey, "/")
	if len(parts) == 3 {
		return AuthEdgeNode, parts[0], parts[1] + "/" + parts[2]
	}

	parts = strings.Split(appKey, "@")
	if len(parts) != 2 && len(parts) != 3 {
		return AuthFailed, "", ""
	}

	var user string
	if len(parts) == 2 {
		user = parts[0]
	} else {
		user = parts[0] + "@" + parts[1]
	}

	for _, regUser := range auth.regularUsers {
		if regUser == user {
			return AuthUser, parts[len(parts)-1], user
		}
	}

	for _, syncAdmin := range auth.syncAdmins {
		if syncAdmin == user {
			return AuthSyncAdmin, "", user
		}
	}

	return AuthAdmin, parts[len(parts)-1], user
}

// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func (auth *DummyAuthenticate) KeyandSecretForURL(url string) (string, string) {
	if strings.HasPrefix(url, common.HTTPCSSURL) {
		return common.Configuration.OrgID + "/" + common.Configuration.DestinationType + "/" +
			common.Configuration.DestinationID, ""
	}
	return "", ""
}
