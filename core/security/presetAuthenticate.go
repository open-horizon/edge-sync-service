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

// PresetAuthenticate is an implementation of the Authenticate interface that uses a set of
// ids defined in the file {PersistenceRootPath}/sync/preset-auth.json.
//
// The file {PersistenceRootPath}/sync/preset-auth.json is of the form:
//
//    {
//      "credentials": {
//        "edgeNodeKey1": {
//          "secret": "edgeNodeSecret1", "orgID": "orgid", "username": "destType/destID", "type": "EdgeNode"
//        },
//        "appKey1": {
//          "secret": "appSecret1", "orgID": "orgid", "username": "user1", "type": "admin"
//        }
//      },
//      "cssCredentials": { "key": "edgeNodeKey1", "secret": "edgeNodeSecret1" }
//    }
//
// The credentials field is a set of JSON objects, whose key or name is an appKey. The JSON objects have
// fields in them for the user's appSecret, the org they are part of, their user name and their type. The
// values for the type field are admin, edgenode, user, syncadmin, and service. The username field for
// an edge node is of the form destType/destID and for a service it is of the form serviceOrg/version/serviceName.
//
// The cssCredentials field is used to provide an ESS with the credentials it needs to communicate with the CSS
// via HTTP. These credentials must be in one of the elements of the above described credentials field on the CSS.
type PresetAuthenticate struct {
	Credentials map[string]CredentialInfo `json:"credentials"`

	// CSSCredentials is the credentials to use when communicating with the CSS
	CSSCredentials CSSCredentials `json:"cssCredentials"`
}

const presetAuthFilename = "/sync/preset-auth.json"

// CredentialInfo is the information related to an app key
type CredentialInfo struct {
	Username string `json:"username"`
	Secret   string `json:"secret"`
	OrgID    string `json:"orgID"`
	Type     string `json:"type"`
	code     int
}

// CSSCredentials defines the appkey and appsecret used to communicate with the CSS
type CSSCredentials struct {
	AppKey    string `json:"key"`
	AppSecret string `json:"secret"`
}

// Start initializes the PresetAuthenticate struct
func (auth *PresetAuthenticate) Start() {
	if auth.Credentials == nil && 0 == len(auth.CSSCredentials.AppKey) {
		authFile, err := os.Open(common.Configuration.PersistenceRootPath + presetAuthFilename)
		if err != nil {
			if log.IsLogging(logger.WARNING) {
				log.Warning("Failed to open user file. Error: %s\n", err)
			}
			auth.Credentials = make(map[string]CredentialInfo)
			return
		}
		decoder := json.NewDecoder(authFile)
		var info PresetAuthenticate
		err = decoder.Decode(&info)
		if err == nil {
			auth.Credentials = info.Credentials
			auth.CSSCredentials = info.CSSCredentials

			for key, info := range auth.Credentials {
				var code int
				switch strings.ToLower(info.Type) {
				case "admin":
					code = AuthAdmin
				case "edgenode":
					code = AuthEdgeNode
				case "user":
					code = AuthUser
				case "syncadmin":
					code = AuthSyncAdmin
				case "service":
					code = AuthService
				}
				info.code = code
				auth.Credentials[key] = info
			}
		} else {
			auth.Credentials = make(map[string]CredentialInfo)
		}
	}
	return
}

// Authenticate  authenticates a particular HTTP request and indicates
// whether it is an edge node, org admin, or plain user. Also returned is the
// user's org and identitity. An edge node's identity is destType/destID. A
// service's identity is serviceOrg/version/serviceName.
func (auth *PresetAuthenticate) Authenticate(request *http.Request) (int, string, string) {
	appKey, appSecret, ok := request.BasicAuth()
	if !ok {
		return AuthFailed, "", ""
	}

	if info, ok := auth.Credentials[appKey]; ok {
		if appSecret != info.Secret {
			return AuthFailed, "", ""
		}
		return info.code, info.OrgID, info.Username
	}

	return AuthFailed, "", ""
}

// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func (auth *PresetAuthenticate) KeyandSecretForURL(url string) (string, string) {
	if strings.HasPrefix(url, common.HTTPCSSURL) {
		// Return credentials for communicating with the CSS
		return auth.CSSCredentials.AppKey, auth.CSSCredentials.AppSecret
	}
	return "", ""
}
