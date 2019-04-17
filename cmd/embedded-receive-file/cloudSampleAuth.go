package main

//	cloudSampleAuth implements the Sync Service authentication interface which enables edge nodes and users 
//	to be authenticated when accessing the Sync Service.
//	For cloud applications with embedded CSS cloudSampleAuth is used to validate the credentials (appKey and appSecrete) 
//	that each edge node provides when when it communicates with the CSS. This is done by the Authenticate function.

import (
	"net/http"

	"github.com/open-horizon/edge-sync-service/core/security"
)

// CredentialInfo is the information related to an app key
type CredentialInfo struct {
	Username string `json:"username"`
	Secret   string `json:"secret"`
	OrgID    string `json:"orgID"`
	Type     string `json:"type"`
	code     int
}

// CloudSampleAuth maintains the credentials of edge nodes in a map
type CloudSampleAuth struct {
	Credentials map[string]CredentialInfo `json:"credentials"`
}

// Add edge node entry.
// orgID, destType and destID are those of the ESS (inside the embedded-upload-files)
func (auth *CloudSampleAuth) AddEdgeNode(key string, secret string, orgID string, destType string, destID string) {
	credInfo := CredentialInfo{destType+"/"+destID, secret, orgID, "EdgeNode", security.AuthEdgeNode}
	if auth.Credentials == nil {
		auth.Credentials = make(map[string]CredentialInfo)
	}
	auth.Credentials[key] = credInfo   
} 
// Start() is part of the Sync Service authentication interface. It is used to initialize the authentication implementation.
// Here it is used to initialize the map that stores edge nodes credentials.
func (auth *CloudSampleAuth) Start() {
	if auth.Credentials == nil {
		auth.Credentials = make(map[string]CredentialInfo)
	}
	return
}

// Authenticate is part of the Sync Service authentication interface.
// Authenticate  authenticates a particular HTTP request and indicates
// whether it is an edge node. Also returned is the
// user's org and identitity. An edge node's identity is destType/destID
func (auth *CloudSampleAuth) Authenticate(request *http.Request) (int, string, string) {
	appKey, appSecret, ok := request.BasicAuth()
	if !ok {
		return security.AuthFailed, "", ""
	}

	if info, ok := auth.Credentials[appKey]; ok && appSecret == info.Secret {
		return info.code, info.OrgID, info.Username
	}

	return security.AuthFailed, "", ""
}

// KeyandSecretForURL is part of the Sync Service authentication interface. 
// It is not used by a cloud application with an embedded CSS.
func (auth *CloudSampleAuth) KeyandSecretForURL(url string) (string, string) {
	return "", ""
}
