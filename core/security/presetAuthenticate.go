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

// PresetAuthenticate is an implementation of the Authenticate interface that uses a preset set of ids.
type PresetAuthenticate struct {
	Credentials map[string]CredentialInfo `json:"credentials"`

	// CSSCredentials is the credentials to use when communicating with the CSS
	CSSCredentials CSSCredentials `json:"cssCredentials"`
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
// user's org and identitity. An edge node's identity is destType/destID
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
