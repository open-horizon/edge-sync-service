package main

//	edgeSampleAuth implements the Sync Service authentication interface which enables edge nodes and users to be 
//	authenticated when accessing the Sync Service.
//	For edge applications with embedded ESS edgeSampleAuth is used to pass the credentials (appKey and appSecrete)
//	that are used by the ESS when it communicates with the CSS. This is done by the KeyandSecretForURL function.

import (
	"net/http"

	"github.com/open-horizon/edge-sync-service/core/security"
)


// NodeCredentials defines the appkey and appsecret used by the edge node to authenticate itself with the CSS
type NodeCredentials struct {
	AppKey    string `json:"key"`
	AppSecret string `json:"secret"`
}

// EdgeSampleAuth is an implementation of the Authenticate interface that uses a preset set of ids.
type EdgeSampleAuth struct {
	// NodeCredentials is the credentials to use when communicating with the CSS
	NodeCredentials NodeCredentials `json:"nodeCredentials"`	
}

// Add NodeCredentials.
// key and secret should match those given to the CSS (using AddEdgeNode())
func (auth *EdgeSampleAuth) AddNodeCredentials(key string, secret string) {
	auth.NodeCredentials = NodeCredentials{key, secret}   
} 
// Start is part of the Sync Service authentication interface. It is used to initialize the authentication implementation.
func (auth *EdgeSampleAuth) Start() {
	return
}

// Authenticate is part of the Sync Service authentication interface.
// This function is not called when using an embedded ESS
func (auth *EdgeSampleAuth) Authenticate(request *http.Request) (int, string, string) {
	return security.AuthFailed, "", ""
}

// KeyandSecretForURL is part of the Sync Service authentication interface.
// KeyandSecretForURL returns an app key and an app secret pair to be
// used by the ESS when communicating with the specified URL.
func (auth *EdgeSampleAuth) KeyandSecretForURL(url string) (string, string) {
	return auth.NodeCredentials.AppKey, auth.NodeCredentials.AppSecret
}
