package security

import "net/http"

// Authentication is the interface invoked by the Sync Service for authentication
// related stuff. An implementation of this interface is provided by the code
// starting up the Sync Service to the Sync Service core code.type
type Authentication interface {
	// Authenticate  authenticates a particular HTTP request and indicates
	// whether it is an edge node, org admin, or plain user. Also returned is the
	// user's org and identitity. An edge node's identity is destType/destID
	Authenticate(request *http.Request) (int, string, string)

	// KeyandSecretForURL returns an app key and an app secret pair to be
	// used by the ESS when communicating with the specified URL.
	KeyandSecretForURL(url string) (string, string)

	// Start gives the Authentication implementation a chance to initialize itself
	Start()
}

const (
	// AuthFailed is returned by Authenticate when a call to Athenticate fails
	AuthFailed = iota

	// AuthEdgeNode is returned by Authenticate when the authenticated user is an edge node
	AuthEdgeNode

	// AuthAdmin is returned by Authenticate when the authenticated user is an org admin
	AuthAdmin

	// AuthUser is returned by Authenticate when the authenticated user is a regular user
	AuthUser

	// AuthSyncAdmin is returned by Authenticate when the authenticated user is a Sync Service Admin
	AuthSyncAdmin
)
