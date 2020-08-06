package security

import (
	"net/http"
	"strings"

	"github.com/open-horizon/edge-sync-service/common"
)

// TestAuthenticate is the test implementation of the Authenticate interface.
type TestAuthenticate struct {
}

// Authenticate  authenticates a particular HTTP request and indicates
// whether it is an edge node, org admin, or plain user. Also returned is the
// user's org and identitity. An edge node's identity is destType/destID. A
// service's identity is serviceOrg/arch/version/serviceName.
//
// Note: This Authenticate implementation is for running the tests. App secrets
//      are ignored. App keys for APIs are of the form, userID@orgID. It supports
//      the following users:
//          testerUser - A regular user
//          testerAdmin - An admin of the specified orgID
//          testSyncAdmin - An admin of the Sync Service
//          testerService1 - A service
//          testerService2 - A service
//      Edge node app keys are of the form orgID/destType/destID
func (auth *TestAuthenticate) Authenticate(request *http.Request) (int, string, string) {
	appKey, _, ok := request.BasicAuth()
	if !ok {
		return AuthFailed, "", ""
	}

	parts := strings.Split(appKey, "/")
	if len(parts) == 3 {
		return AuthEdgeNode, parts[0], parts[1] + "/" + parts[2]
	}

	parts = strings.Split(appKey, "@")
	if len(parts) != 2 {
		return AuthFailed, "", ""
	}

	user := parts[0]
	orgID := parts[1]
	var code int
	if user == "testerUser" || user == "testerUser1" || user == "testerUser2" || user == "testerUserCanAccessAllTypes" {
		code = AuthUser
	} else if user == "testerNode" || user == "testerNode1" || user == "testerNode2" || user == "testerNodeCanAccessAllTypes" {
		code = AuthNodeUser
	} else if user == "testerAdmin" {
		code = AuthAdmin
	} else if user == "testerSyncAdmin" {
		code = AuthSyncAdmin
	} else if user == "testerService1" {
		code = AuthService
		// serviceOrg/version/serviceName
		user = "plover/0.0.1/testerService1"
	} else if user == "testerService2" {
		code = AuthService
		user = "plover/0.0.1/testerService2"
	} else if user == "testerService2b" {
		code = AuthService
		user = "plover/0.1.0/testerService2"
	} else {
		code = AuthFailed
	}

	return code, orgID, user
}

// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func (auth *TestAuthenticate) KeyandSecretForURL(url string) (string, string) {
	if strings.HasPrefix(url, common.HTTPCSSURL) {
		return common.Configuration.OrgID + "/" + common.Configuration.DestinationType + "/" +
			common.Configuration.DestinationID, ""
	}
	return "", ""
}

// Start initializes the Test Authentication implementation
func (auth *TestAuthenticate) Start() {}
