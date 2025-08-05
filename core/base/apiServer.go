package base

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

const destinationsURL = "/api/v1/destinations"
const objectsURL = "/api/v1/objects/"
const organizationURL = "/api/v1/organizations/"
const getOrganizationsURL = "/api/v1/organizations"
const resendURL = "/api/v1/resend"
const securityURL = "/api/v1/security/"
const shutdownURL = "/api/v1/shutdown"
const healthURL = "/api/v1/health"

const (
	contentType     = "Content-Type"
	applicationJSON = "application/json"
)

var unauthorizedBytes = []byte("Unauthorized")

// objectUpdate includes the object's metadata and data
// A sync service object includes metadata and optionally binary data.
// When an object is created the metadata must be provided. The metadata and the data can then be updated together or one at a time.
// swagger:model
type objectUpdate struct {
	// Meta is the object's metadata
	Meta common.MetaData `json:"meta"`

	// Data is a the object's binary data
	Data []byte `json:"data"`
}

// webhookUpdate includes the webhook's action and URL
// A webhook can be used to allow the sync service to invoke actions when new information becomes available.
// An application can choose between using a webhook and periodically polling the sync service for updates.
// swagger:model
type webhookUpdate struct {
	// Action is an action can be either register (create/update a webhook) or delete (delete the webhook)
	Action string `json:"action"`

	// URL is the URL to invoke when new information for the object is available
	URL string `json:"url"`
}

// organization includes the organization's id and broker address
// swagger:model
type organization struct {
	// Organization ID
	OrgID string `json:"org-id"`

	// Broker address
	Address string `json:"address"`
}

// bulkACLUpdate is the payload used when performing a bulk update on an ACL (either adding uses to an
// ACL or removing users from an ACL.
// swagger:model
type bulkACLUpdate struct {
	// Action is an action, which can be either add (to add users) or remove (to remove users)
	Action string `json:"action"`

	// aclUsers is an array of ACL entries to be added or removed from the ACL as appropriate. Don's specify ACLRole if action is "remove"
	Users []common.ACLentry `json:"users"`
}

// bulkDestUpdate is the payload used when need to add or remove destinations for an object
// swagger:model
type bulkDestUpdate struct {
	Action string `json:"action"`

	// Destinations is an array of destinations, each entry is an string in form of "<destinationType>:<destinationID>"
	Destinations []string `json:"destinations"`
}

func setupAPIServer() {
	if common.Configuration.NodeType == common.CSS {
		http.Handle(destinationsURL+"/", http.StripPrefix(destinationsURL+"/", http.HandlerFunc(handleDestinations)))
		http.Handle(securityURL, http.StripPrefix(securityURL, http.HandlerFunc(handleSecurity)))
	} else {
		http.HandleFunc(destinationsURL, handleDestinations)
	}
	http.Handle(objectsURL, http.StripPrefix(objectsURL, http.HandlerFunc(handleObjects)))
	http.HandleFunc(shutdownURL, handleShutdown)
	http.HandleFunc(resendURL, handleResend)
	http.Handle(getOrganizationsURL, http.StripPrefix(getOrganizationsURL, http.HandlerFunc(handleGetOrganizations)))
	http.Handle(organizationURL, http.StripPrefix(organizationURL, http.HandlerFunc(handleOrganizations)))
	http.HandleFunc(healthURL, handleHealth)
}

func handleDestinations(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	code, userOrg, _ := security.Authenticate(request)
	if code == security.AuthFailed || code == security.AuthEdgeNode {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var orgID string
	var parts []string
	if len(request.URL.Path) != 0 {
		parts = strings.Split(request.URL.Path, "/")
		if common.Configuration.NodeType == common.CSS {
			if len(parts) == 0 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			orgID = parts[0]
			parts = parts[1:]
		} else {
			orgID = common.Configuration.OrgID
		}
	} else {
		if common.Configuration.NodeType == common.CSS {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		orgID = common.Configuration.OrgID
	}

	if pathParamValid := validatePathParam(writer, orgID, "", "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	if userOrg != orgID && code != security.AuthSyncAdmin {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if len(parts) == 0 || (len(parts) == 1 && len(parts[0]) == 0) {
		// swagger:operation GET /api/v1/destinations/{orgID} handleDestinations
		//
		// List all known destinations.
		//
		// Provides a list of destinations for an organization, i.e., ESS nodes (belonging to orgID) that have registered with the CSS.
		// This is a CSS only API.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// produces:
		// - application/json
		// - text/plain
		//
		// parameters:
		// - name: orgID
		//   in: path
		//   description: The orgID of the destinations to return.
		//   required: true
		//   type: string
		//
		// responses:
		//   '200':
		//     description: Destinations response
		//     schema:
		//       type: array
		//       items:
		//         "$ref": "#/definitions/Destination"
		//   '404':
		//     description: No destinations found
		//     schema:
		//       type: string
		//   '500':
		//     description: Failed to retrieve the destinations
		//     schema:
		//       type: string
		if dests, err := ListDestinations(orgID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to fetch the list of destinations. Error: ", 0)
		} else {
			if len(dests) == 0 {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				if data, err := json.MarshalIndent(dests, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal the list of destinations. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	} else if len(parts) == 3 || (len(parts) == 4 && len(parts[3]) == 0) && parts[2] == "objects" {
		// swagger:operation GET /api/v1/destinations/{orgID}/{destType}/{destID}/objects handleDestinationObjects
		//
		// List all objects that are in use by the destination.
		//
		// Provides a list of objects that are in use by the destination ESS node.
		// This is a CSS only API.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// produces:
		// - application/json
		// - text/plain
		//
		// parameters:
		// - name: orgID
		//   in: path
		//   description: The orgID of the destination to retrieve objects for.
		//   required: true
		//   type: string
		// - name: destType
		//   in: path
		//   description: The destType of the destination to retrieve objects for.
		//   required: true
		//   type: string
		// - name: destID
		//   in: path
		//   description: The destID of the destination to retrieve objects for.
		//   required: true
		//   type: string
		//
		// responses:
		//   '200':
		//     description: Objects response
		//     schema:
		//       type: array
		//       items:
		//         "$ref": "#/definitions/ObjectStatus"
		//   '404':
		//     description: No objects found
		//     schema:
		//       type: string
		//   '500':
		//     description: Failed to retrieve the objects
		//     schema:
		//       type: string
		if pathParamValid := validatePathParam(writer, orgID, "", "", parts[0], parts[1]); !pathParamValid {
			// header and message are set in function validatePathParam
			return
		}
		if objects, err := GetObjectsForDestination(orgID, parts[0], parts[1]); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to fetch the objects. Error: ", 0)
		} else {
			if len(objects) == 0 {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				if data, err := json.MarshalIndent(objects, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal the list of objects. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	} else {
		writer.WriteHeader(http.StatusBadRequest)
	}
}

// swagger:operation POST /api/v1/resend handleResend
//
// Request to resend objects.
//
// Used by an ESS to ask the CSS to resend it all the objects (supported only for ESS to CSS requests).
// An application only needs to use this API in case the data it previously obtained from the ESS was lost.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// responses:
//   '204':
//     description: The request will be sent
//     schema:
//       type: string
//   '400':
//     description: The request is not allowed on Cloud Sync-Service
//     schema:
//       type: string
func handleResend(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	code, _, _ := security.Authenticate(request)
	if code != security.AuthAdmin && code != security.AuthUser && code != security.AuthSyncAdmin {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if request.Method == http.MethodPost {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleResend\n")
		}
		if err := ResendObjects(); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to send resend objects request. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// POST /api/v1/shutdown?essunregister=true
func handleShutdown(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	code, _, _ := security.Authenticate(request)
	if code != security.AuthSyncAdmin {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if request.Method == http.MethodPost {
		writer.WriteHeader(http.StatusNoContent)

		restart := strings.ToLower(request.URL.Query().Get("restart"))
		quiesceString := request.URL.Query().Get("quiesce")

		essUnregister := false
		if common.Configuration.NodeType == common.ESS {
			unregisterString := request.URL.Query().Get("essunregister")
			var err error
			if unregisterString != "" {
				essUnregister, err = strconv.ParseBool(unregisterString)
				if err != nil {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
			}
		}

		go func() {
			timer := time.NewTimer(time.Duration(1) * time.Second)
			<-timer.C

			quieceTime := 3
			if len(quiesceString) != 0 {
				var quieceTemp int
				_, err := fmt.Sscanf(quiesceString, "%d", &quieceTemp)
				if err == nil {
					quieceTime = quieceTemp
				}
			}

			if restart == "true" || restart == "yes" {
				// If BlockUntilShutdown was called, don't let Stop() unblock
				blocking := waitingOnBlockChannel
				waitingOnBlockChannel = false
				Stop(quieceTime, essUnregister)

				if log.IsLogging(logger.INFO) {
					log.Info("Restarting the Sync Service")
				}
				Start("", false)
				waitingOnBlockChannel = blocking
			} else {
				Stop(quieceTime, essUnregister)
			}
		}()
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handleObjects(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if len(request.URL.Path) != 0 {
		/* Note: request.URL.Path is the parts after /api/vi/objects,
		 * ESS URLs following /api/v1/objects?xxx=xxx will not be handled in this if section and 400 will be returned.
		 * eg:
		 *   GET     /api/v1/objects/orgID?destination_policy=true
		 *   GET     /api/v1/objects/orgID?filters=true
		 *   GET     /api/v1/objects/orgID?list_object_type=true
		 */

		parts := strings.Split(request.URL.Path, "/")
		var orgID string
		if common.Configuration.NodeType == common.CSS {
			orgID = parts[0]
			parts = parts[1:]
		} else {
			orgID = common.Configuration.OrgID
		}

		if len(parts) == 0 {
			// GET     /api/v1/objects/orgID?destination_policy=true
			// GET     /api/v1/objects/orgID?filters=true
			// GET     /api/v1/objects/orgID?list_object_type=true
			if request.Method != http.MethodGet {
				writer.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// GET     /api/v1/objects/orgID?destination_policy=true, return []common.ObjectDestinationPolicy
			destPolicyString := request.URL.Query().Get("destination_policy")
			destPolicy := false

			objectFilterString := request.URL.Query().Get("filters")
			objectFilter := false

			listObjTypeString := request.URL.Query().Get("list_object_type")
			listObjType := false
			var err error
			if destPolicyString != "" {
				destPolicy, err = strconv.ParseBool(destPolicyString)
				if err == nil && destPolicy {
					handleListObjectsWithDestinationPolicy(orgID, writer, request)
					return
				}
			} else if objectFilterString != "" {
				// GET     /api/v1/objects/orgID?filters=true&param1=value1&param2=value2.....
				// this API is to filter objects which have destination policy, and return []common.Metatdata
				objectFilter, err = strconv.ParseBool(objectFilterString)
				if err == nil && objectFilter {
					handleListObjectsWithFilters(orgID, writer, request)
					return
				}
			} else if listObjTypeString != "" {
				// GET     /api/v1/objects/orgID?list_object_type=true
				listObjType, err = strconv.ParseBool(listObjTypeString)
				if err == nil && listObjType {
					handleListObjectTypes(orgID, writer, request)
					return
				}

			}
			writer.WriteHeader(http.StatusBadRequest)
			return
		} else if len(parts) == 1 || (len(parts) == 2 && len(parts[1]) == 0) {
			// /api/v1/objects/orgID/type
			// GET - get updated objects
			// PUT - register/delete a webhook
			switch request.Method {
			case http.MethodGet:

				allObjectsString := request.URL.Query().Get("all_objects")
				allObjects := false
				if allObjectsString != "" {
					var err error
					allObjects, err = strconv.ParseBool(allObjectsString)
					if err != nil {
						writer.WriteHeader(http.StatusBadRequest)
						return
					}
				}
				if allObjects {
					handleListAllObjects(orgID, parts[0], writer, request)
				} else {
					receivedString := request.URL.Query().Get("received")
					received := false
					if receivedString != "" {
						var err error
						received, err = strconv.ParseBool(receivedString)
						if err != nil {
							writer.WriteHeader(http.StatusBadRequest)
							return
						}
					}
					handleListUpdatedObjects(orgID, parts[0], received, writer, request)
				}
			case http.MethodPut:
				handleWebhook(orgID, parts[0], writer, request)
			default:
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}

		} else if len(parts) == 2 || (len(parts) == 3 && len(parts[2]) == 0) {
			// GET/DELETE/PUT /api/v1/objects/orgID/type/id
			handleObjectRequest(orgID, parts[0], parts[1], writer, request)

		} else if len(parts) == 3 || (len(parts) == 4 && len(parts[3]) == 0) {
			// PUT     /api/v1/objects/orgID/type/id/consumed
			// PUT     /api/v1/objects/orgID/type/id/deleted
			// PUT     /api/v1/objects/orgID/type/id/received
			// PUT     /api/v1/objects/orgID/type/id/activate
			// GET     /api/v1/objects/orgID/type/id/status
			// GET/PUT /api/v1/objects/orgID/type/id/data
			// GET/PUT/POST /api/v1/objects/orgID/type/id/destinations
			operation := strings.ToLower(parts[2])
			handleObjectOperation(operation, orgID, parts[0], parts[1], writer, request)

		} else {
			writer.WriteHeader(http.StatusBadRequest)
		}
	} else {
		writer.WriteHeader(http.StatusBadRequest)
	}
}

func handleObjectRequest(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	// We need to check XSS for orgID before sending the value to security component
	if pathParamValid := validatePathParam(writer, orgID, objectType, objectID, "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	setResponseHeaders(writer)

	switch request.Method {

	// swagger:operation GET /api/v1/objects/{orgID}/{objectType}/{objectID} handleGetObjectCSS
	//
	// Get an object.
	//
	// Get the metadata of an object of the specified object type and object ID.
	// The metadata indicates if the objects includes data which can then be obtained using the appropriate API.
	//
	// ---
	//
	// tags:
	// - CSS
	//
	// produces:
	// - application/json
	// - text/plain
	//
	// parameters:
	// - name: orgID
	//   in: path
	//   description: The orgID of the object to return.
	//   required: true
	//   type: string
	// - name: objectType
	//   in: path
	//   description: The object type of the object to return
	//   required: true
	//   type: string
	// - name: objectID
	//   in: path
	//   description: The object ID of the object to return
	//   required: true
	//   type: string
	//
	// responses:
	//   '200':
	//     description: Object response
	//     schema:
	//       "$ref": "#/definitions/MetaData"
	//   '404':
	//     description: Object not found
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to retrieve the object
	//     schema:
	//       type: string

	// ======================================================================================

	// swagger:operation GET /api/v1/objects/{objectType}/{objectID} handleGetObjectESS
	//
	// Get an object.
	//
	// Get the metadata of an object of the specified object type and object ID.
	// The metadata indicates if the objects includes data which can then be obtained using the appropriate API.
	//
	// ---
	//
	// tags:
	// - ESS
	//
	// produces:
	// - application/json
	// - text/plain
	//
	// parameters:
	// - name: objectType
	//   in: path
	//   description: The object type of the object to return
	//   required: true
	//   type: string
	// - name: objectID
	//   in: path
	//   description: The object ID of the object to return
	//   required: true
	//   type: string
	//
	// responses:
	//   '200':
	//     description: Object response
	//     schema:
	//       "$ref": "#/definitions/MetaData"
	//   '404':
	//     description: Object not found
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to retrieve the object
	//     schema:
	//       type: string
	case http.MethodGet:
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Get %s %s\n", objectType, objectID)
		}
		canAccessAllObjects, code, userID := canUserAccessObject(request, orgID, objectType, objectID, false)
		if code == security.AuthFailed {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}
		if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
		} else {
			if metaData == nil {
				if canAccessAllObjects {
					writer.WriteHeader(http.StatusNotFound)
				} else {
					writer.WriteHeader(http.StatusForbidden)
					writer.Write(unauthorizedBytes)
				}

			} else {
				if metaData.Public || canAccessAllObjects {
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In handleObjects. Object is public %t, given user can access all object for object type(%s) %t\n", metaData.Public, objectType, canAccessAllObjects)
					}
					if data, err := json.MarshalIndent(metaData, "", "  "); err != nil {
						communications.SendErrorResponse(writer, err, "Failed to marshal metadata. Error: ", 0)
					} else {
						writer.Header().Add(contentType, applicationJSON)
						writer.WriteHeader(http.StatusOK)
						if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
							log.Error("Failed to write response body, error: " + err.Error())
						}
					}
				} else {
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In handleObjects. Given user %s doesn't have access to object %s %s in org %s\n", userID, objectType, objectID, orgID)
					}
					writer.WriteHeader(http.StatusForbidden)
					writer.Write(unauthorizedBytes)
				}

			}
		}

	// swagger:operation DELETE /api/v1/objects/{orgID}/{objectType}/{objectID} handleDeleteObjectDeleteCSS
	//
	// Delete an object.
	//
	// Delete the object of the specified object type and object ID.
	// Destinations of the object will be notified that the object has been deleted.
	//
	// ---
	//
	// tags:
	// - CSS
	//
	// produces:
	// - text/plain
	//
	// parameters:
	// - name: orgID
	//   in: path
	//   description: The orgID of the object to delete.
	//   required: true
	//   type: string
	// - name: objectType
	//   in: path
	//   description: The object type of the object to delete
	//   required: true
	//   type: string
	// - name: objectID
	//   in: path
	//   description: The object ID of the object to delete
	//   required: true
	//   type: string
	//
	// responses:
	//   '204':
	//     description: Object deleted
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to delete the object
	//     schema:
	//       type: string

	// ======================================================================================

	// swagger:operation DELETE /api/v1/objects/{objectType}/{objectID} handleDeleteObjectDeleteESS
	//
	// Delete an object.
	//
	// Delete the object of the specified object type and object ID.
	// Destinations of the object will be notified that the object has been deleted.
	//
	// ---
	//
	// tags:
	// - ESS
	//
	// produces:
	// - text/plain
	//
	// parameters:
	// - name: objectType
	//   in: path
	//   description: The object type of the object to delete
	//   required: true
	//   type: string
	// - name: objectID
	//   in: path
	//   description: The object ID of the object to delete
	//   required: true
	//   type: string
	//
	// responses:
	//   '204':
	//     description: Object deleted
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to delete the object
	//     schema:
	//       type: string
	case http.MethodDelete:
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Delete %s %s\n", objectType, objectID)
		}
		// authObjectAdmin will return as authAdmin in canUserAccessObject because they have same level of read access
		// side-effect: all auth code (except authSyncAdmin) will need to be checked against security.CanUserCreateObject() function
		if _, code, _ := canUserAccessObject(request, orgID, objectType, objectID, false); code == security.AuthFailed {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		} else if common.Configuration.NodeType == common.CSS && code != security.AuthSyncAdmin {
			// Retrieve metadata, check object type and destination types againest acls
			if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
				return
			} else {
				if metaData == nil {
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				validateUser, userOrgID, userID := security.CanUserCreateObject(request, orgID, metaData)
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleObjects. Delete %s %s. ValidateUser %t %s %s\n", objectType, objectID, validateUser, userOrgID, userID)
				}
				if !validateUser {
					writer.WriteHeader(http.StatusForbidden)
					writer.Write(unauthorizedBytes)
					return
				}
			}
		}

		if err := DeleteObject(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to delete the object. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}

	case http.MethodPut:
		handleUpdateObject(orgID, objectType, objectID, writer, request)

	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}
// swagger:operation GET /api/v1/objects/{orgID}?filters=true handleListObjectsWithFiltersGet
//
// Get objects satisfy the given filters
//
// Get the list of objects that satisfy the given filters
// This is a CSS only API.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//   - name: orgID
//     in: path
//     description: The orgID of the updated objects to return. Present only when working with a CSS, removed from the path when working with an ESS
//     required: true
//     type: string
//   - name: filters
//     in: query
//     description: Must be true to indicate that objects with filters are to be retrieved
//     required: true
//     type: boolean
//   - name: destinationPolicy
//     in: query
//     description: Must be true to indicate that objects with destinationPolicy are to be retrieved
//     required: false
//     type: boolean
//   - name: dpService
//     in: query
//     description: The ID of the service (orgID/serviceName) to which objects have affinity, whose Destination Policy should be fetched.
//     required: false
//     type: string
//   - name: dpPropertyName
//     in: query
//     description: The property name defined inside destination policy to which objects have affinity, whose Destination Policy should be fetched.
//     required: false
//     type: string
//   - name: since
//     in: query
//     description: Objects that have a Destination Policy which was updated since the specified timestamp in RFC3339 should be fetched.
//     required: false
//     type: string
//   - name: objectType
//     in: query
//     description: Fetch the objects with given object type
//     required: false
//     type: string
//   - name: objectID
//     in: query
//     description: Fetch the objects with given object id
//     required: false
//     type: string
//   - name: destinationType
//     in: query
//     description: Fetch the objects with given destination type
//     required: false
//     type: string
//   - name: destinationID
//     in: query
//     description: Fetch the objects with given destination id
//     required: false
//     type: string
//   - name: noData
//     in: query
//     description: Specify true or false. Fetch the objects with noData marked to true/false. If not specified, object will not be filtered by "noData" field
//     required: false
//     type: string
//   - name: expirationTimeBefore
//     in: query
//     description: Fetch the objects with expiration time before specified timestamp in RFC3339 format
//     required: false
//     type: string
//   - name: deleted
//     in: query
//     description: Specify true or false. Fetch the object with deleted marked to true/false. If not specified, object will not be filtered by "deleted" field
//     required: false
//     type: string
//
// responses:
//   '200':
//     description: Objects response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/MetaData"
//   '404':
//     description: No objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the objects
//     schema:
//       type: string

func handleListObjectsWithFilters(orgID string, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListObjectsWithFilters")
	}

	// We need to check XSS for orgID before sending the value to security component
	if pathParamValid := validatePathParam(writer, orgID, "", "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	// only allow AuthSyncAdmin, AuthAdmin, AuthUser and AuthNodeUser to access, it is okay if orgID != userOrgID to display "public" object
	code, userOrgID, userID := security.Authenticate(request)
	if code == security.AuthFailed || (code != security.AuthSyncAdmin && code != security.AuthAdmin && code != security.AuthObjectAdmin && code != security.AuthUser && code != security.AuthNodeUser) {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	destinationPolicyString := request.URL.Query().Get("destinationPolicy")
	var destinationPolicy *bool
	if destinationPolicyString != "" {
		var err error
		destinationPolicyValue, err := strconv.ParseBool(destinationPolicyString)
		destinationPolicy = &destinationPolicyValue
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	dpServiceOrgID := ""
	dpServiceName := ""
	dpPropertyName := ""
	since := int64(0)

	if destinationPolicy != nil && *destinationPolicy == true {
		dpServiceID := request.URL.Query().Get("dpService")
		if dpServiceID != "" {
			parts := strings.SplitN(dpServiceID, "/", 2)
			if len(parts) < 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			dpServiceOrgID = parts[0]
			dpServiceName = parts[1]
		}

		dpPropertyName = request.URL.Query().Get("dpPropertyName")

		if pathParamValid := validatePathParamForService(writer, dpServiceOrgID, dpServiceName, dpPropertyName); !pathParamValid {
			// header and message are set in function validatePathParam
			return
		}

		sinceString := request.URL.Query().Get("since")
		if sinceString != "" {
			var err error
			sinceTime, err := time.Parse(time.RFC3339, sinceString)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

			since = sinceTime.UTC().UnixNano()
			if since < 1 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

		}
	}

	objectType := request.URL.Query().Get("objectType")
	objectID := ""

	if objectType != "" {
		objectID = request.URL.Query().Get("objectID")
	}

	destinationType := request.URL.Query().Get("destinationType")
	destinationID := ""
	if destinationType != "" {
		destinationID = request.URL.Query().Get("destinationID")
	}

	if pathParamValid := validatePathParam(writer, orgID, objectType, objectID, destinationType, destinationID); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	var noData *bool
	noDataString := request.URL.Query().Get("noData")
	if noDataString != "" {
		var err error
		noDataVelue, err := strconv.ParseBool(noDataString)
		noData = &noDataVelue
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	expirationTimeBeforeString := request.URL.Query().Get("expirationTimeBefore")
	if expirationTimeBeforeString != "" {
		_, err := time.Parse(time.RFC3339, expirationTimeBeforeString)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	var deleted *bool
	deletedString := request.URL.Query().Get("deleted")
	if deletedString != "" {
		deletedValue, err := strconv.ParseBool(deletedString)
		deleted = &deletedValue
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	var objects []common.MetaData
	var err error

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListObjectsWithFilters, get objects with %s %s %s %s %s %d %s %s %s %s %s %s %s\n", orgID, destinationPolicyString, dpServiceOrgID, dpServiceName, dpPropertyName, since, objectType, objectID, destinationType, destinationID, noDataString, expirationTimeBeforeString, deletedString)
	}

	if objects, err = ListObjectsWithFilters(orgID, destinationPolicy, dpServiceOrgID, dpServiceName, dpPropertyName, since, objectType, objectID, destinationType, destinationID, noData, expirationTimeBeforeString, deleted); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of objects with given conditions. Error: ", 0)
	} else {
		if len(objects) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if accessibleObjects, err := GetAccessibleObjects(code, orgID, userOrgID, userID, objects, objectType); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to get accessible object. Error: ", 0)
			} else {
				if data, err := json.MarshalIndent(accessibleObjects, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal the list of objects with a Metadata. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	}
}
// swagger:operation GET /api/v1/objects/{orgID}?list_object_type=true handleListObjectTypes
//
// Get objects types
//
// Get the list of objects types under a given org
// This is a CSS only API.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//   - name: orgID
//     in: path
//     description: The orgID of the object types to retrieve
//     required: true
//     type: string
//   - name: list_object_type
//     in: query
//     description: Must be true to indicate that object types are to be retrieved
//     required: true
//     type: boolean
//
// responses:
//   '200':
//     description: Objects response
//     schema:
//       type: array
//       items:
//         type: string
//   '404':
//     description: No objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the object types
//     schema:
//       type: string

func handleListObjectTypes(orgID string, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListObjectTypes")
	}

	// We need to check XSS for orgID before sending the value to security component
	if pathParamValid := validatePathParam(writer, orgID, "", "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	// only allow AuthSyncAdmin, AuthAdmin, AuthUser and AuthNodeUser to access, it is okay if orgID != userOrgID to display "public" object
	code, userOrgID, userID := security.Authenticate(request)
	if code == security.AuthFailed || (code != security.AuthSyncAdmin && code != security.AuthAdmin && code != security.AuthObjectAdmin && code != security.AuthUser && code != security.AuthNodeUser) {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	var objects []common.MetaData
	var objTypes []string
	var err error

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListObjectTypes, get objects types under %s\n", orgID)
	}

	if objects, err = ListObjectsWithFilters(orgID, nil, "", "", "", int64(0), "", "", "", "", nil, "", nil); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of objects with given conditions. Error: ", 0)
	} else {
		if len(objects) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if accessibleObjects, err := GetAccessibleObjects(code, orgID, userOrgID, userID, objects, ""); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to get accessible object. Error: ", 0)
			} else {
				objTypes = GetListOfObjTypes(accessibleObjects)
				if data, err := json.MarshalIndent(objTypes, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal the list of object types. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	}
}

func handleObjectOperation(operation string, orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	var canAccessAllObjects bool
	var code int
	var userID string

	if pathParamValid := validatePathParam(writer, orgID, objectType, objectID, "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	if operation != "deleted" {
		canAccessAllObjects, code, userID = canUserAccessObject(request, orgID, objectType, objectID, false)
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjectOperation, given user %s with authcode %d canAccessAllObjects: %t\n", userID, code, canAccessAllObjects)
		}
		if code == security.AuthFailed {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		if operation == "consumed" || operation == "policyreceived" || operation == "received" || operation == "activate" {
			// all "mark" status API will forbidden user that only have access to "public" object
			if !canAccessAllObjects {
				writer.WriteHeader(http.StatusForbidden)
				writer.Write(unauthorizedBytes)
				return
			}
		}

	}

	switch operation {
	case "consumed":
		handleObjectConsumed(orgID, objectType, objectID, writer, request)
	case "deleted":
		handleObjectDeleted(orgID, objectType, objectID, writer, request)
	case "policyreceived":
		handlePolicyReceived(orgID, objectType, objectID, writer, request)
	case "received":
		handleObjectReceived(orgID, objectType, objectID, writer, request)
	case "activate":
		handleActivateObject(orgID, objectType, objectID, writer, request)
	case "status":
		handleObjectStatus(orgID, objectType, objectID, canAccessAllObjects, writer, request)
	case "destinations":
		handleObjectDestinations(orgID, objectType, objectID, canAccessAllObjects, writer, request)
	case "data":
		switch request.Method {
		case http.MethodGet:
			handleObjectGetData(orgID, objectType, objectID, canAccessAllObjects, writer, request)

		case http.MethodPut:
			handleObjectPutData(orgID, objectType, objectID, writer, request)

		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	default:
		writer.WriteHeader(http.StatusBadRequest)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/consumed handleObjectConsumedCSS
//
// Mark an object as consumed.
//
// Mark the object of the specified object type and object ID as having been consumed by the application.
// After the object is marked as consumed it will not be delivered to the application again (even if the sync service is restarted).
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object to mark as consumed.
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object to mark as consumed
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object to mark as consumed
//   required: true
//   type: string
//
// responses:
//   '204':
//     description: Object marked as consumed
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object consumed
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID}/consumed handleObjectConsumedESS
//
// Mark an object as consumed.
//
// Mark the object of the specified object type and object ID as having been consumed by the application.
// After the object is marked as consumed it will not be delivered to the application again (even if the sync service is restarted).
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object to mark as consumed
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to mark as consumed
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: Object marked as consumed
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object consumed
//     schema:
//       type: string
func handleObjectConsumed(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodPut {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Consumed %s %s\n", objectType, objectID)
		}
		if err := ObjectConsumed(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to mark the object as consumed. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/deleted handleObjectDeletedCSS
//
// The service confirms object deletion.
//
// Confirm the deletion of the object of the specified object type and object ID by the application.
// The application should invoke this API after it completed the actions associated with deleting the object.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object to confirm its deletion. Present only when working with a CSS, removed from the path when working with an ESS
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object to confirm its deletion
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object to confirm its deletion
//   required: true
//   type: string
//
// responses:
//   '204':
//     description: Object's deletion confirmed
//     schema:
//       type: string
//   '500':
//     description: Failed to confirm the object's deletion
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID}/deleted handleObjectDeletedESS
//
// The service confirms object deletion.
//
// Confirm the deletion of the object of the specified object type and object ID by the application.
// The application should invoke this API after it completed the actions associated with deleting the object.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object to confirm its deletion
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to confirm its deletion
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: Object's deletion confirmed
//     schema:
//       type: string
//   '500':
//     description: Failed to confirm the object's deletion
//     schema:
//       type: string

func handleObjectDeleted(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	canAccessAllObjects, code, serviceID := canUserAccessObject(request, orgID, objectType, objectID, true)
	if code == security.AuthFailed {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if request.Method == http.MethodPut {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Deleted %s %s\n", objectType, objectID)
		}
		// all "mark" status API will forbidden user that only have access to "public" object
		if !canAccessAllObjects {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return

		}
		if err := ObjectDeleted(serviceID, orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to confirm object's deletion. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/policyreceived handlePolicyReceived
//
// Mark an object's destination policy as having been received. This is a CSS only API
//
// Mark the object of the specified object type and object ID as having its destination policy received.
// After the object is marked as such it will not be delivered to the application listing objects with
// a destination policy, unless it adds "received=true" to the query parameters.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: orgID
//     in: path
//     description: The orgID of the object to mark as having its destination policy received.
//     required: true
//     type: string
//   - name: objectType
//     in: path
//     description: The object type of the object to mark as having its destination policy received.
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to mark as having its destination policy received.
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: Object marked as having its destination policy received
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object as having its destination policy received
//     schema:
//       type: string
func handlePolicyReceived(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodPut {
		if common.Configuration.NodeType == common.ESS {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Policy Received %s %s\n", objectType, objectID)
		}
		if err := ObjectPolicyReceived(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to mark the object's destination policy as having been received. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/received handleObjectReceivedCSS
//
// Mark an object as received.
//
// Mark the object of the specified object type and object ID as having been received by the application.
// After the object is marked as received it will only be delivered to the application again if specified in the objects request.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object to mark as received
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object to mark as received
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object to mark as received
//   required: true
//   type: string
//
// responses:
//   '204':
//     description: Object marked as received
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object received
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID}/received handleObjectReceivedESS
//
// Mark an object as received.
//
// Mark the object of the specified object type and object ID as having been received by the application.
// After the object is marked as received it will only be delivered to the application again if specified in the objects request.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object to mark as received
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to mark as received
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: Object marked as received
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object received
//     schema:
//       type: string
func handleObjectReceived(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodPut {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Received %s %s\n", objectType, objectID)
		}
		if err := ObjectReceived(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to mark the object as received. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/activate handleActivateObjectPutCSS
//
// Mark an object as active.
//
// Mark the object of the specified object type and object ID as active.
// An object can be created as inactive which means it is not delivered to its destinations.
// This API is used to activate such objects and initiate the distribution of the object to its destinations.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object to mark as active
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object to mark as active
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object to mark as active
//   required: true
//   type: string
//
// responses:
//   '204':
//     description: Object marked as active
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object active
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID}/activate handleActivateObjectPutESS
//
// Mark an object as active.
//
// Mark the object of the specified object type and object ID as active.
// An object can be created as inactive which means it is not delivered to its destinations.
// This API is used to activate such objects and initiate the distribution of the object to its destinations.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object to mark as active
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to mark as active
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: Object marked as active
//     schema:
//       type: string
//   '500':
//     description: Failed to mark the object active
//     schema:
//       type: string

func handleActivateObject(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodPut {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Activate %s %s\n", objectType, objectID)
		}
		if err := ActivateObject(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "Failed to mark the object as active. Error: ", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation GET /api/v1/objects/{orgID}/{objectType}/{objectID}/status handleObjectStatusCSS
//
// Get the status of an object.
//
// Get the status of the object of the specified object type and object ID.
// The status can be one of the following:
//   notReady - The object is not ready to be sent to destinations.
//   ready - The object is ready to be sent to destinations.
//   received - The object's metadata has been received but not all its data.
//   completelyReceived - The full object (metadata and data) has been received.
//   consumed - The object has been consumed by the application.
//   deleted - The object was deleted.
//
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object whose status will be retrieved
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object whose status will be retrieved
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object whose status will be retrieved
//   required: true
//   type: string
//
// responses:
//   '200':
//     description: Object status
//     schema:
//       type: string
//       enum: [notReady, ready, received, completelyReceived, consumed, deleted]
//   '500':
//     description: Failed to retrieve the object's status
//     schema:
//       type: string

// ======================================================================================

// swagger:operation GET /api/v1/objects/{objectType}/{objectID}/status handleObjectStatusESS
//
// Get the status of an object.
//
// Get the status of the object of the specified object type and object ID.
// The status can be one of the following:
//
//	notReady - The object is not ready to be sent to destinations.
//	ready - The object is ready to be sent to destinations.
//	received - The object's metadata has been received but not all its data.
//	completelyReceived - The full object (metadata and data) has been received.
//	consumed - The object has been consumed by the application.
//	deleted - The object was deleted.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object whose status will be retrieved
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object whose status will be retrieved
//     required: true
//     type: string
//
// responses:
//   '200':
//     description: Object status
//     schema:
//       type: string
//       enum: [notReady, ready, received, completelyReceived, consumed, deleted]
//   '500':
//     description: Failed to retrieve the object's status
//     schema:
//       type: string
func handleObjectStatus(orgID string, objectType string, objectID string, canAccessAllObjects bool, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Get status of %s %s\n", objectType, objectID)
		}
		// if given user only have access to public object, and the retrieved object is private object, return 403
		if !canAccessAllObjects {
			if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
				return
			} else if metaData == nil || !metaData.Public {
				writer.WriteHeader(http.StatusForbidden)
				writer.Write(unauthorizedBytes)
				return
			}
		}

		if status, err := GetObjectStatus(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
		} else {
			if status == "" {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				writer.Header().Add(contentType, "plain/text")
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write([]byte(status)); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handleObjectDestinations(orgID string, objectType string, objectID string, canAccessAllObjects bool, writer http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		// swagger:operation GET /api/v1/objects/{orgID}/{objectType}/{objectID}/destinations handleObjectDestinations
		//
		// Get the destinations of an object.
		//
		// Get the list of sync service (ESS) nodes which are the destinations of the object of the specified object type and object ID.
		// The delivery status of the object is provided for each destination along with its type and ID.
		// This is a CSS only API.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// produces:
		// - text/plain
		//
		// parameters:
		// - name: orgID
		//   in: path
		//   description: The orgID of the object whose destinations will be retrieved. Present only when working with a CSS, removed from the path when working with an ESS
		//   required: true
		//   type: string
		// - name: objectType
		//   in: path
		//   description: The object type of the object whose destinations will be retrieved
		//   required: true
		//   type: string
		// - name: objectID
		//   in: path
		//   description: The object ID of the object whose destinations will be retrieved
		//   required: true
		//   type: string
		//
		// responses:
		//   '200':
		//     description: Object destinations and their status
		//     schema:
		//       type: array
		//       items:
		//         "$ref": "#/definitions/DestinationsStatus"
		//   '500':
		//     description: Failed to retrieve the object's destinations
		//     schema:
		//       type: string
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Get destinations of %s %s\n", objectType, objectID)
		}
		// if given user only have access to public object, and the retrieved object is private object, return 403
		if !canAccessAllObjects {
			if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
				return
			} else if metaData == nil || !metaData.Public {
				writer.WriteHeader(http.StatusForbidden)
				writer.Write(unauthorizedBytes)
				return
			}
		}

		if dests, err := GetObjectDestinationsStatus(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
		} else {
			if dests == nil {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				if destinations, err := json.MarshalIndent(dests, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal object's destinations. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write([]byte(destinations)); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	} else if request.Method == http.MethodPut {
		// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/destinations handleObjectDestinationsUpdatePut
		//
		// Set the destinations of an object.
		//
		// Set the list of sync service (ESS) nodes to be the destinations of the object of the specified object type and object ID.
		// This is a CSS only API.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// consumes:
		// - application/json
		//
		// produces:
		// - text/plain
		//
		// parameters:
		// - name: orgID
		//   in: path
		//   description: The orgID of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: objectType
		//   in: path
		//   description: The object type of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: objectID
		//   in: path
		//   description: The object ID of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: payload
		//   in: body
		//   description: The object's destination list
		//   required: true
		//   schema:
		//      type: array
		//      items:
		//         type: string
		//
		// responses:
		//   '204':
		//     description: Object destinations updated
		//     schema:
		//       type: string
		//   '500':
		//     description: Failed to update the object's destinations
		//     schema:
		//       type: string
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. Set destinations of %s %s\n", objectType, objectID)
		}
		if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
		} else {
			if metaData == nil {
				writer.WriteHeader(http.StatusNotFound)
				return
			}

			validateUser, userOrgID, userID := security.CanUserCreateObject(request, orgID, metaData)
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In handleObjectDestinations. validateUser %t %s %s\n", validateUser, userOrgID, userID)
			}
			if !validateUser {
				writer.WriteHeader(http.StatusForbidden)
				writer.Write(unauthorizedBytes)
				return
			}
			var destinationsList []string
			err := json.NewDecoder(request.Body).Decode(&destinationsList)
			inputValidated, validateErr := common.ValidateDestinationListInput(destinationsList)
			if inputValidated && err == nil {
				if err := UpdateObjectDestinations(orgID, objectType, objectID, destinationsList); err == nil {
					writer.WriteHeader(http.StatusNoContent)
				} else {
					communications.SendErrorResponse(writer, err, "", 0)
				}
			} else if !inputValidated {
				communications.SendErrorResponse(writer, validateErr, "Unsupported char in destinationsList. Error: ", http.StatusBadRequest)
			} else {
				communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
			}
		}

	} else if request.Method == http.MethodPost {
		// swagger:operation POST /api/v1/objects/{orgID}/{objectType}/{objectID}/destinations handleObjectDestinationsUpdatePost
		//
		// Add/Delete the destinations of an object.
		//
		// Add or delete the list of sync service (ESS) nodes to/from be the destinations of the object of the specified object type and object ID.
		// This is a CSS only API.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// consumes:
		// - application/json
		//
		// produces:
		// - text/plain
		//
		// parameters:
		// - name: orgID
		//   in: path
		//   description: The orgID of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: objectType
		//   in: path
		//   description: The object type of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: objectID
		//   in: path
		//   description: The object ID of the object whose destinations will be updated
		//   required: true
		//   type: string
		// - name: payload
		//   in: body
		//   description: The object's destination list to add or remove
		//   required: true
		//   schema:
		//      "$ref": "#/definitions/bulkDestUpdate"
		//
		// responses:
		//   '204':
		//     description: Object destinations updated
		//     schema:
		//       type: string
		//   '500':
		//     description: Failed to update the object's destinations
		//     schema:
		//       type: string
		var payload bulkDestUpdate
		err := json.NewDecoder(request.Body).Decode(&payload)
		if err == nil {
			var updateErr error
			dests := payload.Destinations
			if strings.EqualFold(payload.Action, common.RemoveAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleObjectDestinations. Bulk remove destinations for %s, %s, %s \n", orgID, objectType, objectID)
				}
				updateErr = DeleteObjectDestinations(orgID, objectType, objectID, dests)
			} else if strings.EqualFold(payload.Action, common.AddAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleObjectDestinations. Bulk add destinations for %s, %s, %s \n", orgID, objectType, objectID)
				}
				updateErr = AddObjectDestinations(orgID, objectType, objectID, dests)
			} else {
				communications.SendErrorResponse(writer, nil, fmt.Sprintf("Invalid action (%s) in payload.", payload.Action), http.StatusBadRequest)
				return
			}

			if updateErr == nil {
				writer.WriteHeader(http.StatusNoContent)
			} else {
				communications.SendErrorResponse(writer, updateErr, "", 0)
			}

		} else {
			communications.SendErrorResponse(writer, err, "Invalid JSON for add/remove destinations. Error: ", http.StatusBadRequest)
		}

	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation GET /api/v1/objects/{orgID}/{objectType}/{objectID}/data handleObjectGetDataGetCSS
//
// Get the data of an object.
//
// Get the data of the object of the specified object type and object ID.
// The metadata of the object indicates if the object includes data (noData is false).
//
// ---
//
// tags:
// - CSS
//
// produces:
// - application/octet-stream
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object whose data will be retrieved
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object whose data will be retrieved
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object whose data will be retrieved
//   required: true
//   type: string
//
// responses:
//   '200':
//     description: Object data
//     schema:
//       type: string
//       format: binary
//   '500':
//     description: Failed to retrieve the object's data
//     schema:
//       type: string

// ======================================================================================

// swagger:operation GET /api/v1/objects/{objectType}/{objectID}/data handleObjectGetDataGetESS
//
// Get the data of an object.
//
// Get the data of the object of the specified object type and object ID.
// The metadata of the object indicates if the object includes data (noData is false).
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - application/octet-stream
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object whose data will be retrieved
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object whose data will be retrieved
//     required: true
//     type: string
//
// responses:
//   '200':
//     description: Object data
//     schema:
//       type: string
//       format: binary
//   '500':
//     description: Failed to retrieve the object's data
//     schema:
//       type: string
func handleObjectGetData(orgID string, objectType string, objectID string, canAccessAllObjects bool, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleObjects. Get data %s %s, canAccessAllObjects %t\n", objectType, objectID, canAccessAllObjects)
	}

	// if given user only have access to public object, and the retrieved object is private object, return 403
	if !canAccessAllObjects {
		if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
			return
		} else if metaData == nil || !metaData.Public {
			communications.SendErrorResponse(writer, nil, "Unauthorized. The object may not exist or be public.", http.StatusForbidden)
			return
		}
	}

	// Get range from the header "Range:bytes={startOffset}-{endOffset}"
	var dataReader io.Reader
	var eof bool
	var objSize int64
	startOffset, endOffset, err := common.GetStartAndEndRangeFromRangeHeader(request)
	if err != nil {
		communications.SendErrorResponse(writer, err, "", 0)
	}

	var requestPartialData bool
	if startOffset == -1 && endOffset == -1 {
		// Range header not specified, will get all data
		requestPartialData = false
		dataReader, err = GetObjectData(orgID, objectType, objectID)
	} else {
		requestPartialData = true
		objSize, dataReader, eof, _, err = GetObjectDataByChunk(orgID, objectType, objectID, startOffset, endOffset)
		if (endOffset - startOffset + 1) >= objSize {
			// requested range >= object size
			requestPartialData = false
		}
	}

	if err != nil {
		communications.SendErrorResponse(writer, err, "", 0)
	} else {
		if dataReader == nil {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			writer.Header().Add(contentType, "application/octet-stream")
			if requestPartialData {
				if eof {
					endOffset = objSize - 1
				}
				writer.Header().Add("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startOffset, endOffset, objSize))
			}

			if requestPartialData {
				writer.WriteHeader(http.StatusPartialContent)
			} else {
				writer.WriteHeader(http.StatusOK)
			}
			if _, err := io.Copy(writer, dataReader); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
			}
			if err := store.CloseDataReader(dataReader); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
			}
		}
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID}/data handleObjectPutDataCSS
//
// Update the data of an object.
//
// Update the data of the object of the specified object type and object ID.
// The data can be updated without modifying the object's metadata.
//
// ---
//
// tags:
// - CSS
//
// consumes:
// - application/octet-stream
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object whose data will be updated
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object whose data will be updated
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object whose data will be updated
//   required: true
//   type: string
// - name: payload
//   in: body
//   description: The object's new data. When read data bytes from a file, please set application/octet-stream as Content-Type in header.
//   required: true
//   schema:
//     type: string
//     format: binary
//
// responses:
//   '204':
//     description: Object data updated
//     schema:
//       type: string
//   '404':
//     description: The specified object doesn't exist
//     schema:
//       type: string
//   '500':
//     description: Failed to update the object's data
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID}/data handleObjectPutDataESS
//
// Update the data of an object.
//
// Update the data of the object of the specified object type and object ID.
// The data can be updated without modifying the object's metadata.
//
// ---
//
// tags:
//   - ESS
//
// consumes:
//   - application/octet-stream
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object whose data will be updated
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object whose data will be updated
//     required: true
//     type: string
//   - name: payload
//     in: body
//     description: The object's new data. When reading data bytes from a file, please set application/octet-stream as Content-Type in header.
//     required: true
//     schema:
//       type: string
//       format: binary
//
// responses:
//   '204':
//     description: Object data updated
//     schema:
//       type: string
//   '404':
//     description: The specified object doesn't exist
//     schema:
//       type: string
//   '500':
//     description: Failed to update the object's data
//     schema:
//       type: string
func handleObjectPutData(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleObjects. Update data %s %s\n", objectType, objectID)
	}

	// Retrieve metadata
	if metaData, err := GetObject(orgID, objectType, objectID); err != nil {
		communications.SendErrorResponse(writer, err, "", 0)
	} else {
		if metaData == nil {
			writer.WriteHeader(http.StatusNotFound)
			return
		}

		validateUser, userOrgID, userID := security.CanUserCreateObject(request, orgID, metaData)
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjectPutData. validateUser %t %s %s\n", validateUser, userOrgID, userID)
		}
		if !validateUser {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		totalSize, startOffset, endOffset, err := common.GetStartAndEndRangeFromContentRangeHeader(request)
		if err != nil {
			reqErr := &common.InvalidRequest{Message: fmt.Sprintf("Failed to parse Content-Range header, Error: %s", err.Error())}
			communications.SendErrorResponse(writer, reqErr, "", 0)
			return
		}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjectPutData. TotalSize: %d, startOffset: %d, endOffset: %d\n", totalSize, startOffset, endOffset)
		}

		var found bool
		var chunkUpload bool
		if totalSize == 0 && startOffset == -1 && endOffset == -1 {
			found, err = PutObjectAllData(orgID, objectType, objectID, request.Body)
			chunkUpload = false
		} else {
			found, err = PutObjectChunkData(orgID, objectType, objectID, request.Body, startOffset, endOffset, totalSize)
			chunkUpload = true
		}

		if err == nil {
			if !found {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				if chunkUpload {
					writer.Header().Add("MMS-Upload-Owner", leader.GetLeaderID())
				}
				writer.WriteHeader(http.StatusNoContent)
			}
		} else {
			communications.SendErrorResponse(writer, err, "", 0)
		}
	}

}

// swagger:operation GET /api/v1/objects/{orgID}/{objectType} handleListObjectsGetCSS
//
// Get objects of the specified type.
//
// Get objects of the specified object type. Either get all of the objects or just those objects that have pending (unconsumed) updates.
// An application would typically invoke the latter API periodically to check for updates (an alternative is to use a webhook).
//
// ---
//
// tags:
// - CSS
//
// produces:
// - application/json
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the objects to return
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the objects to return
//   required: true
//   type: string
// - name: received
//   in: query
//   description: When returning updated objects only, whether or not to include the objects that have been marked as received by the application
//   required: false
//   type: boolean
//
// responses:
//   '200':
//     description: Updated objects response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/MetaData"
//   '404':
//     description: No updated objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string

// ======================================================================================

// swagger:operation GET /api/v1/objects/{objectType} handleListObjectsGetESS
//
// Get objects of the specified type.
//
// Get objects of the specified object type. Either get all of the objects or just those objects that have pending (unconsumed) updates.
// An application would typically invoke the latter API periodically to check for updates (an alternative is to use a webhook).
//
// ---
//
// tags:
// - ESS
//
// produces:
// - application/json
// - text/plain
//
// parameters:
// - name: objectType
//   in: path
//   description: The object type of the objects to return
//   required: true
//   type: string
// - name: received
//   in: query
//   description: When returning updated objects only, whether or not to include the objects that have been marked as received by the application
//   required: false
//   type: boolean
//
// responses:
//   '200':
//     description: Updated objects response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/MetaData"
//   '404':
//     description: No updated objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string

func handleListUpdatedObjects(orgID string, objectType string, received bool, writer http.ResponseWriter,
	request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListUpdatedObjects. List %s, Method %s, orgID %s, objectType %s. Include received %t\n",
			objectType, request.Method, orgID, objectType, received)
	}

	if pathParamValid := validatePathParam(writer, orgID, objectType, "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	canAccessAllObjects, code, userID := canUserAccessObject(request, orgID, objectType, "", true) //objectID == "", so checkLastDestinationPolicyServices will not be used
	if code == security.AuthFailed {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}
	if metaData, err := ListUpdatedObjects(orgID, objectType, received); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of updates. Error: ", 0)
	} else {
		var result []common.MetaData
		// CSS or ESS (expect for AuthService)
		if common.Configuration.NodeType == common.CSS || code != security.AuthService {
			if canAccessAllObjects {
				result = metaData
			} else {
				// only show public
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleListUpdatedObjects. Given user %s %d can only access public objects for objectType %s in org %s\n",
						userID, code, objectType, orgID)
				}
				for _, objMeta := range metaData {
					if objMeta.Public {
						result = append(result, objMeta)
					}
				}
			}
		} else {
			// ESS && AuthService
			result = make([]common.MetaData, 0)
			for _, object := range metaData {
				//removedDestinationPolicyServices := []common.ServiceID{}
				if removedDestinationPolicyServices, err := GetRemovedDestinationPolicyServicesFromESS(orgID, object.ObjectType, object.ObjectID); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to fetch removedDestinationPolicyServices for object. Error: ", 0)
				} else {
					if canServiceAccessObject(userID, object.DestinationPolicy, removedDestinationPolicyServices, true) {
						if serviceContainedInLastDestinationPolicyServices(userID, removedDestinationPolicyServices) {
							shadowObj := object
							shadowObj.Deleted = true
							result = append(result, shadowObj)
							if trace.IsLogging(logger.DEBUG) {
								trace.Debug("In handleObjects. object.deleted: %t, shadowObj.deleted: %t for object %s %s\n",
									object.Deleted, shadowObj.Deleted, object.ObjectType, object.ObjectID)
							}
						} else {
							result = append(result, object)
						}
					}
				}

			}
		}

		if len(result) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if data, err := json.MarshalIndent(result, "", "  "); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to marshal the list of updates. Error: ", 0)
			} else {
				writer.Header().Add(contentType, applicationJSON)
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}
	}
}

// swagger:operation GET /api/v1/objects/{orgID}/{objectType}?all_objects=true handleListAllObjectsCSS
//
// Get objects with a destination policy of the specified type.
//
// Get all objects of the specified object type that have a destination policy.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - application/json
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the objects to return
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the objects to return
//   required: true
//   type: string
// - name: all_objects
//   in: query
//   description: Whether or not to include all objects. If false only updated objects will be returned.
//   required: true
//   type: boolean
//
// responses:
//   '200':
//     description: Objects with a destination policy response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/ObjectDestinationPolicy"
//   '404':
//     description: No objects with a destination policy found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string

// ======================================================================================

// swagger:operation GET /api/v1/objects/{objectType}?all_objects=true handleListAllObjectsESS
//
// Get objects with a destination policy of the specified type.
//
// Get all objects of the specified object type that have a destination policy.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the objects to return
//     required: true
//     type: string
//   - name: all_objects
//     in: query
//     description: Whether or not to include all objects. If false only updated objects will be returned.
//     required: true
//     type: boolean
//
// responses:
//   '200':
//     description: Objects with a destination policy response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/ObjectDestinationPolicy"
//   '404':
//     description: No objects with a destination policy found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string
func handleListAllObjects(orgID string, objectType string, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListAllObjects. List %s, Method %s, orgID %s, objectType %s\n",
			objectType, request.Method, orgID, objectType)
	}

	if pathParamValid := validatePathParam(writer, orgID, objectType, "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	canAccessAllObjects, code, userID := canUserAccessObject(request, orgID, objectType, "", false)
	if code == security.AuthFailed {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if objects, err := ListAllObjects(orgID, objectType); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of objects. Error: ", 0)
	} else {
		var result []common.ObjectDestinationPolicy
		// CSS or ESS (expect for AuthService)
		if common.Configuration.NodeType == common.CSS || code != security.AuthService {
			if canAccessAllObjects {
				result = objects
			} else {
				// only show public
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleListAllObjects. Given user %s %d can only access public objects for objectType %s in org %s\n",
						userID, code, objectType, orgID)
				}
				var metaData *common.MetaData
				for _, objDestPolicy := range objects {
					if metaData, err = GetObject(objDestPolicy.OrgID, objDestPolicy.ObjectType, objDestPolicy.ObjectID); err != nil {
						communications.SendErrorResponse(writer, err, "Failed to get entire object from object with destination policy", 0)
						return
					}
					if metaData != nil && metaData.Public {
						result = append(result, objDestPolicy)
					}
				}
			}

		} else {
			// ESS && AuthService
			result = make([]common.ObjectDestinationPolicy, 0)
			oldPolicyServices := make([]common.ServiceID, 0)
			for _, object := range objects {
				if canServiceAccessObject(userID, object.DestinationPolicy, oldPolicyServices, false) {
					result = append(result, object)
				}
			}
		}

		if len(result) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if data, err := json.MarshalIndent(result, "", "  "); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to marshal the list of objects. Error: ", 0)
			} else {
				writer.Header().Add(contentType, applicationJSON)
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}
	}
}

// swagger:operation GET /api/v1/objects/{orgID}?destination_policy=true handleListObjectsWithDestinationPolicyGetCSS
//
// Get objects that have destination policies.
//
// Get the list of objects that have destination policies.
// An application would typically invoke this API periodically to check for updates.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - application/json
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the updated objects to return
//   required: true
//   type: string
// - name: destination_policy
//   in: query
//   description: Must be true to indicate that objects with destinationPolicy are to be retrieved
//   required: true
//   type: boolean
// - name: received
//   in: query
//   description: Whether or not to include the objects that have been marked as received by the application
//   required: false
//   type: boolean
// - name: service
//   in: query
//   description: The ID of the service (orgID/serviceName) to which objects have affinity, whose Destination Policy should be fetched.
//   required: false
//   type: string
// - name: since
//   in: query
//   description: Objects that have a Destination Policy which was updated since the specified UTC time in nanoseconds should be fetched.
//   required: false
//   type: integer
//   format: int64
//
// responses:
//   '200':
//     description: Object destination policy response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/ObjectDestinationPolicy"
//   '404':
//     description: No updated objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string

// ======================================================================================

// swagger:operation GET /api/v1/objects?destination_policy=true handleListObjectsWithDestinationPolicyGetESS
//
// Get objects that have destination policies.
//
// Get the list of objects that have destination policies.
// An application would typically invoke this API periodically to check for updates.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//   - name: destination_policy
//     in: query
//     description: Must be true to indicate that objects with destinationPolicy are to be retrieved
//     required: true
//     type: boolean
//   - name: received
//     in: query
//     description: Whether or not to include the objects that have been marked as received by the application
//     required: false
//     type: boolean
//   - name: service
//     in: query
//     description: The ID of the service (orgID/serviceName) to which objects have affinity, whose Destination Policy should be fetched.
//     required: false
//     type: string
//   - name: since
//     in: query
//     description: Objects that have a Destination Policy which was updated since the specified UTC time in nanoseconds should be fetched.
//     required: false
//     type: integer
//     format: int64
//
// responses:
//   '200':
//     description: Object destination policy response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/ObjectDestinationPolicy"
//   '404':
//     description: No updated objects found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the updated objects
//     schema:
//       type: string

func handleListObjectsWithDestinationPolicy(orgID string, writer http.ResponseWriter,
	request *http.Request) {
	code, userOrgID, userID := security.Authenticate(request)
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleListObjectsWithDestinationPolicy. code: %d, userOrgID: %s, userID: %s\n", code, userOrgID, userID)
	}

	// only allow AuthSyncAdmin, AuthAdmin, AuthUser and AuthNodeUser to access, it is okay if orgID != userOrgID to display "public" object
	if code == security.AuthFailed || (code != security.AuthSyncAdmin && code != security.AuthAdmin && code != security.AuthObjectAdmin && code != security.AuthUser && code != security.AuthNodeUser) {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	receivedString := request.URL.Query().Get("received")
	received := false
	if receivedString != "" {
		var err error
		received, err = strconv.ParseBool(receivedString)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	serviceOrgID := ""
	serviceName := ""
	serviceID := request.URL.Query().Get("service")
	if serviceID != "" {
		parts := strings.SplitN(serviceID, "/", 2)
		if len(parts) < 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		serviceOrgID = parts[0]
		serviceName = parts[1]

		if valid := validatePathParamForService(writer, serviceOrgID, serviceName, ""); !valid {
			// header and message are set in function validatePathParam
			return
		}
	}

	since := int64(0)
	sinceString := request.URL.Query().Get("since")
	if sinceString != "" {
		var err error
		since, err = strconv.ParseInt(sinceString, 10, 64)
		if err != nil || since < 1 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	var objects []common.ObjectDestinationPolicy
	var err error

	if since != 0 {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. List DestinationPolicy, orgID %s. since %d\n",
				orgID, since)
		}
		objects, err = ListObjectsWithDestinationPolicyUpdatedSince(orgID, since)
	} else if serviceName == "" {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. List DestinationPolicy, orgID %s. Include received %t\n",
				orgID, received)
		}
		objects, err = ListObjectsWithDestinationPolicy(orgID, received)
	} else {
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. List DestinationPolicy, orgID %s, service %s/%s/\n",
				orgID, serviceOrgID, serviceName)
		}
		objects, err = ListObjectsWithDestinationPolicyByService(orgID, serviceOrgID, serviceName)
	}

	if err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of objects with a DestinationPolicy. Error: ", 0)
	} else {
		if len(objects) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if accessibleObjects, err := GetAccessibleObjectsDestinationPolicy(code, orgID, userOrgID, userID, objects); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to get accessible object. Error: ", 0)
			} else {
				if data, err := json.MarshalIndent(accessibleObjects, "", "  "); err != nil {
					communications.SendErrorResponse(writer, err, "Failed to marshal the list of objects with a DestinationPolicy. Error: ", 0)
				} else {
					writer.Header().Add(contentType, applicationJSON)
					writer.WriteHeader(http.StatusOK)
					if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to write response body, error: " + err.Error())
					}
				}
			}
		}
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType} handleWebhookPutCSS
//
// Register or delete a webhook.
//
// Register or delete a webhook for the specified object type.
// A webhook is used to process notifications on updates for objects of the specified object type.
//
// ---
//
// tags:
// - CSS
//
// consumes:
// - application/json
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the objects for the webhook
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the objects for the webhook
//   required: true
//   type: string
// - name: payload
//   in: body
//   description: The webhook's data
//   required: true
//   schema:
//    "$ref": "#/definitions/webhookUpdate"
//
// responses:
//   '204':
//     description: Webhook registered/deleted
//     schema:
//       type: string
//   '500':
//     description: Failed to update the webhook's data
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType} handleWebhookPutESS
//
// Register or delete a webhook.
//
// Register or delete a webhook for the specified object type.
// A webhook is used to process notifications on updates for objects of the specified object type.
//
// ---
//
// tags:
//   - ESS
//
// consumes:
//   - application/json
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the objects for the webhook
//     required: true
//     type: string
//   - name: payload
//     in: body
//     description: The webhook's data
//     required: true
//     schema:
//       "$ref": "#/definitions/webhookUpdate"
//
// responses:
//   '204':
//     description: Webhook registered/deleted
//     schema:
//       type: string
//   '500':
//     description: Failed to update the webhook's data
//     schema:
//       type: string

func handleWebhook(orgID string, objectType string, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	if pathParamValid := validatePathParam(writer, orgID, objectType, "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	_, code, userID := canUserAccessObject(request, orgID, objectType, "", false)
	if code == security.AuthFailed || code == security.AuthService {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	} else if (code == security.AuthUser || code == security.AuthNodeUser) && common.Configuration.NodeType == common.CSS {
		aclUserType := security.GetACLUserType(code)
		if validateUser := security.CheckObjectCanBeModifiedByUser(userID, orgID, objectType, aclUserType); !validateUser {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}
	}

	var hookErr error
	var payload webhookUpdate
	err := json.NewDecoder(request.Body).Decode(&payload)
	if err == nil {
		if strings.EqualFold(payload.Action, common.DeleteAction) {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In handleObjects. Delete webhook %s\n", objectType)
			}
			hookErr = DeleteWebhook(orgID, objectType, payload.URL)
		} else if strings.EqualFold(payload.Action, common.RegisterAction) {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In handleObjects. Register webhook %s\n", objectType)
			}
			hookErr = RegisterWebhook(orgID, objectType, payload.URL)
		}
		if hookErr == nil {
			writer.WriteHeader(http.StatusNoContent)
		} else {
			communications.SendErrorResponse(writer, hookErr, "", 0)
		}
	} else {
		communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
	}
}

// swagger:operation PUT /api/v1/objects/{orgID}/{objectType}/{objectID} handleUpdateObjectCSS
//
// Update/create an object.
//
// Update/create the object of the specified object type and object ID.
// If an object with the same type and ID exists that object is updated, otherwise a new object is created.
//
// ---
//
// tags:
// - CSS
//
// produces:
// - text/plain
//
// parameters:
// - name: orgID
//   in: path
//   description: The orgID of the object to update/create
//   required: true
//   type: string
// - name: objectType
//   in: path
//   description: The object type of the object to update/create
//   required: true
//   type: string
// - name: objectID
//   in: path
//   description: The object ID of the object to update/create
//   required: true
//   type: string
// - name: payload
//   in: body
//   required: true
//   schema:
//     "$ref": "#/definitions/objectUpdate"
//
// responses:
//   '204':
//     description: Object updated
//     schema:
//       type: string
//   '500':
//     description: Failed to update/create the object
//     schema:
//       type: string

// ======================================================================================

// swagger:operation PUT /api/v1/objects/{objectType}/{objectID} handleUpdateObjectESS
//
// Update/create an object.
//
// Update/create the object of the specified object type and object ID.
// If an object with the same type and ID exists that object is updated, otherwise a new object is created.
//
// ---
//
// tags:
//   - ESS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: objectType
//     in: path
//     description: The object type of the object to update/create
//     required: true
//     type: string
//   - name: objectID
//     in: path
//     description: The object ID of the object to update/create
//     required: true
//     type: string
//   - name: payload
//     in: body
//     required: true
//     schema:
//       "$ref": "#/definitions/objectUpdate"
//
// responses:
//   '204':
//     description: Object updated
//     schema:
//       type: string
//   '500':
//     description: Failed to update/create the object
//     schema:
//       type: string

func handleUpdateObject(orgID string, objectType string, objectID string, writer http.ResponseWriter, request *http.Request) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleObjects. Update %s %s %s\n", orgID, objectType, objectID)
	}

	var payload objectUpdate
	err := json.NewDecoder(request.Body).Decode(&payload)
	if err == nil {
		validateUser, userOrgID, userID := security.CanUserCreateObject(request, orgID, &payload.Meta)
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In handleObjects. validateUser %t %s %s\n", validateUser, userOrgID, userID)
		}
		if !validateUser {
			writer.WriteHeader(http.StatusForbidden)
			writer.Write(unauthorizedBytes)
			return
		}

		// update the ownerID field
		if common.Configuration.NodeType == common.CSS && payload.Meta.OwnerID != userOrgID+"/"+userID {
			payload.Meta.OwnerID = userOrgID + "/" + userID
		}

		if err := UpdateObject(orgID, objectType, objectID, payload.Meta, payload.Data); err == nil {
			writer.WriteHeader(http.StatusNoContent)
		} else {
			communications.SendErrorResponse(writer, err, "", 0)
		}
	} else {
		communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
	}
}

// swagger:operation GET /api/v1/organizations handleGetOrganizations
//
// Get organizations.
//
// Get the list of existing organizations. Relevant to CSS only.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//
// responses:
//   '200':
//     description: Organizations response
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/Organization"
//   '404':
//     description: No organizations found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the organizations
//     schema:
//       type: string

func handleGetOrganizations(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	code, userOrg, _ := security.Authenticate(request)
	if code != security.AuthAdmin && code != security.AuthSyncAdmin {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleGetOrganizations. Get the list of organizations.\n")
	}
	if orgs, err := getOrganizations(); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to fetch the list of organizations. Error: ", 0)
	} else {
		if len(orgs) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			orgsList := make([]organization, 0)
			for _, org := range orgs {
				if code == security.AuthSyncAdmin || userOrg == org.OrgID {
					orgsList = append(orgsList, organization{OrgID: org.OrgID, Address: org.Address})
				}
			}
			if data, err := json.MarshalIndent(orgsList, "", "  "); err != nil {
				communications.SendErrorResponse(writer, err, "Failed to marshal the list of organizations. Error: ", 0)
			} else {
				writer.Header().Add(contentType, applicationJSON)
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}
	}
}

func handleOrganizations(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	var orgID string
	if len(request.URL.Path) == 0 {
		handleGetOrganizations(writer, request)
		return
	}

	parts := strings.Split(request.URL.Path, "/")
	if len(parts) != 1 && !(len(parts) == 2 && len(parts[1]) == 0) {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	orgID = parts[0]

	if pathParamValid := validatePathParam(writer, orgID, "", "", "", ""); !pathParamValid {
		// header and message are set in function validatePathParam
		return
	}

	code, userOrg, _ := security.Authenticate(request)
	if !((code == security.AuthAdmin && orgID == userOrg) || code == security.AuthSyncAdmin) {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	switch request.Method {
	// swagger:operation DELETE /api/v1/organizations/{orgID} handleDeleteOrganization
	//
	// Delete organization.
	//
	// Remove organization information and clean up all resources (objects, destinations, etc.) all resources (objects, destinations, etc.) associated
	// with the deleted organization. Relevant to CSS only.
	//
	// ---
	//
	// tags:
	// - CSS
	//
	// produces:
	// - text/plain
	//
	// parameters:
	// - name: orgID
	//   in: path
	//   description: The orgID of the organization to delete.
	//   required: true
	//   type: string
	//
	// responses:
	//   '204':
	//     description: The organization was successfuly deleted
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to delete the organization
	//     schema:
	//       type: string
	case http.MethodDelete:
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("Deleting organization %s\n", orgID)
		}
		if err := deleteOrganization(orgID); err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
		} else {
			writer.WriteHeader(http.StatusNoContent)
		}

	// swagger:operation PUT /api/v1/organizations/{orgID} handleOrganizations
	//
	// Update organization.
	//
	// Store organization information. Relevant to CSS only.
	//
	// ---
	//
	// tags:
	// - CSS
	//
	// produces:
	// - text/plain
	//
	// parameters:
	// - name: orgID
	//   in: path
	//   description: The orgID of the organization to update.
	//   required: true
	//   type: string
	// - name: payload
	//   in: body
	//   required: true
	//   schema:
	//     "$ref": "#/definitions/Organization"
	//
	// responses:
	//   '204':
	//     description: The organization was successfuly updated
	//     schema:
	//       type: string
	//   '500':
	//     description: Failed to update the organization
	//     schema:
	//       type: string
	case http.MethodPut:
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("Updating organization %s\n", orgID)
		}
		var payload common.Organization
		err := json.NewDecoder(request.Body).Decode(&payload)
		if err == nil {
			if err := updateOrganization(orgID, payload); err != nil {
				communications.SendErrorResponse(writer, err, "", 0)
			} else {
				writer.WriteHeader(http.StatusNoContent)
			}
		} else {
			communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
		}

	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handleSecurity(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	if !common.Running {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	code, userOrg, _ := security.Authenticate(request)
	if code == security.AuthFailed || (code != security.AuthAdmin && code != security.AuthSyncAdmin) {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	parts := strings.Split(request.URL.Path, "/")
	if len(parts) < 2 || len(parts) > 5 {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	aclType := parts[0]
	orgID := parts[1]
	parts = parts[2:]
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleSecurity(), aclType: %s, orgID: %s, len(parts) %d\n", aclType, orgID, len(parts))
	}

	if code != security.AuthSyncAdmin && userOrg != orgID {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	if aclType != common.DestinationsACLType && aclType != common.ObjectsACLType {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	switch request.Method {
	case http.MethodDelete:
		if len(parts) != 3 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		handleACLDelete(aclType, orgID, parts, writer)

	case http.MethodGet:
		if len(parts) > 1 {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		allKeysString := request.URL.Query().Get("all_keys")
		allKeys := false
		if allKeysString != "" {
			var err error
			allKeys, err = strconv.ParseBool(allKeysString)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		objOrDestType := ""
		objOrDestType = request.URL.Query().Get("key")

		//acl user type
		aclUserType := ""
		aclUserType = request.URL.Query().Get("acl_usertype")

		if aclUserType != "" && aclUserType != security.ACLUser && aclUserType != security.ACLNode {
			communications.SendErrorResponse(writer, nil, fmt.Sprintf("Invalid acl user type %s in URL. If specified, the value should be \"user\" or \"node\"", aclUserType), http.StatusBadRequest)
			return
		}

		handleACLGet(aclType, orgID, objOrDestType, aclUserType, writer, allKeys)

	case http.MethodPut:
		handleACLUpdate(request, aclType, orgID, parts, writer)

	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// swagger:operation DELETE /api/v1/security/{type}/{orgID}/{key}/{aclUserType}/{username} handleACLDelete
//
// Remove a user from an ACL for a destination type or an object type.
//
// Remove a user from an ACL for a destination type or an object type. If the last username is removed,
// the ACL is deleted as well.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: type
//     in: path
//     description: The type of the ACL to remove the specified username from.
//     required: true
//     type: string
//     enum: [destinations, objects]
//   - name: orgID
//     in: path
//     description: The orgID in which the ACL for the destination type or object type exists.
//     required: true
//     type: string
//   - name: key
//     in: path
//     description: The destination type or object type that is being protected by the ACL.
//     required: true
//     type: string
//   - name: aclUserType
//     in: path
//     description: The acl user type of given username to be deleted
//     required: true
//     type: string
//     enum: [user, node]
//   - name: username
//     in: path
//     description: The username to remove from the specified ACL.
//     required: true
//     type: string
//
// responses:
//   '204':
//     description: The username was removed from the specified ACL.
//     schema:
//       type: string
//   '500':
//     description: Failed to remove the username from the specified ACL.
//     schema:
//       type: string
func handleACLDelete(aclType string, orgID string, parts []string, writer http.ResponseWriter) {
	if pathParamValid := validatePathParamForSecurity(writer, orgID, parts[0], parts[1], parts[2]); !pathParamValid {
		return
	}

	users := make([]common.ACLentry, 0)
	user := common.ACLentry{
		Username:    parts[2],
		ACLUserType: parts[1],
		ACLRole:     security.ACLReader, // can be any value, this value will not be used when remove acl from database
	}
	users = append(users, user)
	if err := RemoveUsersFromACL(aclType, orgID, parts[0], users); err == nil {
		writer.WriteHeader(http.StatusNoContent)
	} else {
		communications.SendErrorResponse(writer, err, "", 0)
	}
}

func handleACLGet(aclType string, orgID string, key string, aclUserType string, writer http.ResponseWriter, allKeysParam bool) {
	// Get all ACLs

// swagger:operation GET /api/v1/security/{type}/{orgID} handleACLGetAll
//
// Retrieve the list of destination type or object type of ACLs for an organization.
//
// ---
//
// tags:
//   - CSS
//
// produces:
//   - text/plain
//
// parameters:
//   - name: type
//     in: path
//     description: The type of the ACL whose username list should be retrieved.
//     required: true
//     type: string
//     enum: [destinations, objects]
//   - name: orgID
//     in: path
//     description: The orgID in which the ACL for the destination type or object type exists.
//     required: true
//     type: string
//   - name: all_keys
//     in: query
//     description: If true, display object/destination types that has ACL list associated with
//     required: false
//     type: boolean
//   - name: key
//     in: query
//     description: The destination type or object type that is being protected by the ACL.
//     required: false
//     type: string
//   - name: acl_usertype
//     in: query
//     description: The acl user type (user or node) to retrieve.
//     required: false
//     type: string
//
// responses:
//   '200':
//     description: The list of ACLs retrieved of the specified type.
//     schema:
//       type: array
//       items:
//         type: string
//   '404':
//     description: No ACLs found
//     schema:
//       type: string
//   '500':
//     description: Failed to retrieve the list of ACLs retrieved of the specified type.
//     schema:
//       type: string


	if pathParamValid := validatePathParamForSecurity(writer, orgID, key, aclUserType, ""); !pathParamValid {
		return
	}

	requestType := "ACLs"
	if allKeysParam {
		keys, err := RetrieveACLsInOrg(aclType, orgID)
		if err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
			return
		}

		if len(keys) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if data, err := json.MarshalIndent(keys, "", "  "); err != nil {
				message := fmt.Sprintf("Failed to marshal the list of %s. Error: ", requestType)
				communications.SendErrorResponse(writer, err, message, 0)
			} else {
				writer.Header().Add(contentType, applicationJSON)
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}

	} else {
		users, err := RetrieveACL(aclType, orgID, key, aclUserType)
		if err != nil {
			communications.SendErrorResponse(writer, err, "", 0)
			return
		}

		if len(users) == 0 {
			writer.WriteHeader(http.StatusNotFound)
		} else {
			if data, err := json.MarshalIndent(users, "", "  "); err != nil {
				message := fmt.Sprintf("Failed to marshal the list of %s. Error: ", requestType)
				communications.SendErrorResponse(writer, err, message, 0)
			} else {
				writer.Header().Add(contentType, applicationJSON)
				writer.WriteHeader(http.StatusOK)
				if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to write response body, error: " + err.Error())
				}
			}
		}

	}

}

func handleACLUpdate(request *http.Request, aclType string, orgID string, parts []string, writer http.ResponseWriter) {
	if len(parts) == 1 {
		// Bulk add or bulk delete

		// swagger:operation PUT /api/v1/security/{type}/{orgID}/{key} handleBulkACLUpdate
		//
		// Bulk add/remove of username(s) to/from an ACL for a destination type or an object type.
		//
		// Bulk add/remove of username(s) to/from an ACL for a destination type or an object type. If the
		// first username is being added, the ACL is created. If the last username is removed, the ACL
		// is deleted.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// produces:
		// - text/plain
		//
		// parameters:
		// - name: type
		//   in: path
		//   description: The type of the ACL to which the specified user(s) will be added/removed.
		//   required: true
		//   type: string
		//   enum: [destinations, objects]
		// - name: orgID
		//   in: path
		//   description: The orgID in which the ACL for the destination type or object type exists.
		//   required: true
		//   type: string
		// - name: key
		//   in: path
		//   description: The destination type or object type that is being protected by the ACL.
		//   required: true
		//   type: string
		// - name: payload
		//   in: body
		//   required: true
		//   schema:
		//     "$ref": "#/definitions/bulkACLUpdate"
		//
		// responses:
		//   '204':
		//     description: The user(s) were added/removed to/from the specified ACL.
		//     schema:
		//       type: string
		//   '500':
		//     description: Failed to add/remove the user(s)to/from the specified ACL.
		//     schema:
		//       type: string
		if pathParamValid := validatePathParamForSecurity(writer, orgID, parts[0], "", ""); !pathParamValid {
			return
		}

		var payload bulkACLUpdate
		err := json.NewDecoder(request.Body).Decode(&payload)
		if err == nil {

			var updateErr error
			var updatedPayload *[]common.ACLentry
			if strings.EqualFold(payload.Action, common.RemoveAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleSecurity. Bulk remove usernames %s\n", parts[0])
				}
				if err = security.CheckRemoveACLInputFormat(payload.Users); err != nil {
					communications.SendErrorResponse(writer, err, "Invalid ACL entry for update. Error: ", http.StatusBadRequest)
					return
				} else {
					updateErr = RemoveUsersFromACL(aclType, orgID, parts[0], payload.Users)
				}
			} else if strings.EqualFold(payload.Action, common.AddAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleSecurity. Bulk add usernames %s\n", parts[0])
				}
				if updatedPayload, err = security.CheckAddACLInputFormat(aclType, payload.Users); err != nil {
					communications.SendErrorResponse(writer, err, "Invalid ACL entry for update. Error: ", http.StatusBadRequest)
					return
				} else if updatedPayload != nil {
					updateErr = AddUsersToACL(aclType, orgID, parts[0], *updatedPayload)
				} else {
					updateErr = AddUsersToACL(aclType, orgID, parts[0], payload.Users)
				}

			} else {
				communications.SendErrorResponse(writer, nil, fmt.Sprintf("Invalid action (%s) in payload.", payload.Action), http.StatusBadRequest)
				return
			}
			if updateErr == nil {
				writer.WriteHeader(http.StatusNoContent)
			} else {
				communications.SendErrorResponse(writer, updateErr, "", 0)
			}
		} else {
			communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
		}
	} else if len(parts) == 0 {
		// Bulk add or bulk delete ACL username/nodename for all object or destination types

		// swagger:operation PUT /api/v1/security/{type}/{orgID} handleBulkACLUpdateForAllTypes
		//
		// Bulk add/remove of user(s) to/from an ACL for all destination types or all object types.
		//
		// Bulk add/remove of user(s) to/from an ACL for all destination types or all object types. If the
		// first username is being added, the ACL is created. If the last username is removed, the ACL
		// is deleted.
		//
		// ---
		//
		// tags:
		// - CSS
		//
		// produces:
		// - text/plain
		//
		// parameters:
		// - name: type
		//   in: path
		//   description: The type of the ACL to which the specified username(s) will be added/removed.
		//   required: true
		//   type: string
		//   enum: [destinations, objects]
		// - name: orgID
		//   in: path
		//   description: The orgID in which the ACL for the destination type or object type exists.
		//   required: true
		//   type: string
		// - name: payload
		//   in: body
		//   required: true
		//   schema:
		//     "$ref": "#/definitions/bulkACLUpdate"
		//
		// responses:
		//  '204':
		//    description: The username(s) were added/removed to/from the specified ACL.
		//    schema:
		//      type: string
		//  '500':
		//    description: Failed to add/remove the username(s) to/from the specified ACL.
		//    schema:
		//      type: string
		if pathParamValid := validatePathParamForSecurity(writer, orgID, "", "", ""); !pathParamValid {
			return
		}

		var payload bulkACLUpdate
		err := json.NewDecoder(request.Body).Decode(&payload)
		if err == nil {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In handleSecurity. Bulk add/remove usernames for all %s types\n", aclType)
			}

			var updateErr error
			var updatedPayload *[]common.ACLentry
			if strings.EqualFold(payload.Action, common.RemoveAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleSecurity. Bulk remove usernames for all %s types\n", aclType)
				}
				if err = security.CheckRemoveACLInputFormat(payload.Users); err != nil {
					communications.SendErrorResponse(writer, err, "Invalid ACL entry for update. Error: ", http.StatusBadRequest)
					return
				} else {
					updateErr = RemoveUsersFromACL(aclType, orgID, "", payload.Users)
				}
			} else if strings.EqualFold(payload.Action, common.AddAction) {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleSecurity. Bulk add usernames for all %s types\n", aclType)
				}
				// check payload.Usernames in correct format: user:{username}:{aclrole} or node:{nodename}:{aclrole}
				if updatedPayload, err = security.CheckAddACLInputFormat(aclType, payload.Users); err != nil {
					communications.SendErrorResponse(writer, err, "Invalid ACL entry for update. Error: ", http.StatusBadRequest)
					return
				} else if updatedPayload != nil {
					updateErr = AddUsersToACL(aclType, orgID, "", *updatedPayload)
				} else {
					updateErr = AddUsersToACL(aclType, orgID, "", payload.Users)
				}
			} else {
				communications.SendErrorResponse(writer, nil, fmt.Sprintf("Invalid action (%s) in payload.", payload.Action), http.StatusBadRequest)
				return
			}
			if updateErr == nil {
				writer.WriteHeader(http.StatusNoContent)
			} else {
				communications.SendErrorResponse(writer, updateErr, "", 0)
			}
		} else {
			communications.SendErrorResponse(writer, err, "Invalid JSON for update. Error: ", http.StatusBadRequest)
		}
	}
}

func canUserAccessObject(request *http.Request, orgID, objectType, objectID string, checkLastDestinationPolicyServices bool) (bool, int, string) {
	accessToAllObject, code, userID := security.CanUserAccessAllObjects(request, orgID, objectType)
	if code != security.AuthService || common.Configuration.NodeType == common.CSS || objectID == "" {
		return accessToAllObject, code, userID
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In canUserAccessObject. This is ESS and authcode is %d\n", code)
	}

	// ESS & authCode is AuthService
	lockIndex := common.HashStrings(orgID, objectType, objectID)
	apiObjectLocks.RLock(lockIndex)
	defer apiObjectLocks.RUnlock(lockIndex)

	metadata, removedDestinationPolicyServices, err := store.RetrieveObjectAndRemovedDestinationPolicyServices(orgID, objectType, objectID)
	if err == nil && metadata != nil {
		if canServiceAccessObject(userID, metadata.DestinationPolicy, removedDestinationPolicyServices, checkLastDestinationPolicyServices) {
			return true, code, userID
		}

		// else service do not have access to the object, first returned value should be false
		if metadata.Public {
			return false, code, userID
		}
	}
	return false, security.AuthFailed, ""
}

func canServiceAccessObject(serviceID string, policy *common.Policy, oldPolicyServices []common.ServiceID, checkLastDestinationPolicyServices bool) bool {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In canServiceAccessObject. serviceID: %s\n", serviceID)
	}
	if policy == nil || len(policy.Services) == 0 {
		return true
	}
	// serviceOrgID/version/serviceName
	parts := strings.SplitN(serviceID, "/", 3)
	if len(parts) < 3 {
		return false
	}
	serviceFoundInDestinationPolicy := false
	for _, service := range policy.Services {
		if parts[0] == service.OrgID && parts[2] == service.ServiceName {
			if policySemVerRange, err := common.ParseSemVerRange(service.Version); err == nil {
				if serviceSemVer, err := common.ParseSemVer(parts[1]); err == nil {
					if policySemVerRange.IsInRange(serviceSemVer) {
						serviceFoundInDestinationPolicy = true
					}
				}
			}
		}
	}

	if serviceFoundInDestinationPolicy {
		return true
	}

	// If checkLastDestinationPolicyServices flag is true && authentication serviceID is found in oldDestinationPolicyService, this metadata object is accessible
	if checkLastDestinationPolicyServices {
		return serviceContainedInLastDestinationPolicyServices(serviceID, oldPolicyServices)
	}

	return false
}

func serviceContainedInLastDestinationPolicyServices(serviceID string, oldPolicyServices []common.ServiceID) bool {
	// serviceOrgID/version/serviceName
	parts := strings.SplitN(serviceID, "/", 3)
	if len(parts) < 3 {
		return false
	}
	for _, oldService := range oldPolicyServices {
		if parts[0] == oldService.OrgID && parts[2] == oldService.ServiceName {
			if policySemVerRange, err := common.ParseSemVerRange(oldService.Version); err == nil {
				if serviceSemVer, err := common.ParseSemVer(parts[1]); err == nil {
					if policySemVerRange.IsInRange(serviceSemVer) {
						return true
					}
				}
			}
		}
	}
	return false
}

func GetAccessibleObjects(code int, orgID string, userOrgID string, userID string, objects []common.MetaData, objectType string) ([]common.MetaData, error) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In getAccessibleObjects, orgID: %s, userOrgID: %s, userID: %s, objectType: %s\n", orgID, userOrgID, userID, objectType)
	}

	accessibleObjects := make([]common.MetaData, 0)

	if code == security.AuthSyncAdmin || (code == security.AuthAdmin && orgID == userOrgID) || (code == security.AuthObjectAdmin && orgID == userOrgID) {
		// AuthSyncAdmin: show all objects
		// AuthAdmin in this org: show all objects
		accessibleObjects = append(accessibleObjects, objects...)
	} else if orgID != userOrgID {
		// different org: only show public objects
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In GetAccessibleObjects, userOrg %s*is not same as orgID %s in API path, will return public objects\n", userOrgID, orgID)
		}
		for _, objMeta := range objects {
			if objMeta.Public {
				//add object to accessableObjects
				accessibleObjects = append(accessibleObjects, objMeta)
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In handleListObjectsWithFilters, add public object %s %s %s to result\n", objMeta.DestOrgID, objMeta.ObjectType, objMeta.ObjectID)
				}
			}
		}

	} else if code == security.AuthUser || code == security.AuthNodeUser {
		// same org, only can see result by AuthUser or AuthNodeUser, need to check ACL. Return the accessable objects, and public objects
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("Get objects in same org. Check if object type is accessable by given user (%d, %s, %s)\n", code, userID, userOrgID)
		}
		if objectType == common.MANIFEST_OBJECT_TYPE && code == security.AuthNodeUser {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("AuthNodeUser(%d, %s, %s) is getting manifest file\n", code, userID, userOrgID)
			}
			// authNodeUser can read all manifest from its own org
			accessibleObjects = append(accessibleObjects, objects...)
		} else {
			aclUserType := security.GetACLUserType(code)

			if accessToALlTypes, accessibleObjectTypes, err := security.CheckObjectTypesCanBeAccessByGivenUser(orgID, aclUserType, userID); err != nil {
				return make([]common.MetaData, 0), err
			} else if accessToALlTypes {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("Given user (%d, %s, %s) have access to all object types\n", code, userID, userOrgID)
				}
				accessibleObjects = append(accessibleObjects, objects...)
			} else {
				for _, objmeta := range objects {
					if objmeta.Public || common.StringListContains(accessibleObjectTypes, objmeta.ObjectType) {
						if trace.IsLogging(logger.DEBUG) {
							trace.Debug("Object type %s is accessble by user %s:%s in orgID %s\n", objmeta.ObjectType, aclUserType, userID, orgID)
						}
						//add object to accessableObjects
						accessibleObjects = append(accessibleObjects, objmeta)
					} else if objmeta.ObjectType == common.MANIFEST_OBJECT_TYPE && code == security.AuthNodeUser {
						if trace.IsLogging(logger.DEBUG) {
							trace.Debug("Object type %s is accessble by user %s:%s in orgID %s\n", objmeta.ObjectType, aclUserType, userID, orgID)
						}
						//add object to accessableObjects
						accessibleObjects = append(accessibleObjects, objmeta)
					}
				}
			}
		}
	}

	return accessibleObjects, nil
}

func GetAccessibleObjectsDestinationPolicy(code int, orgID string, userOrgID string, userID string, objects []common.ObjectDestinationPolicy) ([]common.ObjectDestinationPolicy, error) {
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In GetAccessibleObjectsDestinationPolicy\n")
	}

	accessibleObjects := make([]common.ObjectDestinationPolicy, 0)

	if code == security.AuthSyncAdmin || (code == security.AuthAdmin && orgID == userOrgID) || (code == security.AuthObjectAdmin && orgID == userOrgID) {
		// AuthSyncAdmin: show all objects
		// AuthAdmin in this org: show all objects
		accessibleObjects = append(accessibleObjects, objects...)
	} else if orgID != userOrgID {
		// different org: only show public objects
		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("UserOrg %s is not same as orgID %s in API path, will return public objects\n", userID, orgID)
		}
		for _, objDestPolicy := range objects {
			if metaData, err := GetObject(objDestPolicy.OrgID, objDestPolicy.ObjectType, objDestPolicy.ObjectID); err != nil {
				// communications.SendErrorResponse(writer, err, "Failed to get entire object from object with destination policy", 0)
				// return
				return make([]common.ObjectDestinationPolicy, 0), err
			} else {
				if metaData.Public {
					//add object to accessableObjects
					accessibleObjects = append(accessibleObjects, objDestPolicy)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Add public object %s %s %s to result\n", objDestPolicy.OrgID, objDestPolicy.ObjectType, objDestPolicy.ObjectID)
					}
				}
			}
		}

	} else if code == security.AuthUser || code == security.AuthNodeUser {
		// same org, only can see result by AuthUser or AuthNodeUser, need to check ACL. Return the accessable objects, and public objects
		aclUserType := security.GetACLUserType(code)

		if accessToALlTypes, accessibleObjectTypes, err := security.CheckObjectTypesCanBeAccessByGivenUser(orgID, aclUserType, userID); err != nil {
			// communications.SendErrorResponse(writer, err, "Get accessable objects for user. Error: ", 0)
			// return
			return make([]common.ObjectDestinationPolicy, 0), err
		} else if accessToALlTypes {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("Given user (%d, %s, %s) have access to all object types\n", code, userID, userOrgID)
			}
			accessibleObjects = append(accessibleObjects, objects...)
		} else {
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("AccessableObjectTypes len is: %d\n", len(accessibleObjectTypes))
			}
			for _, objDestPolicy := range objects {
				if common.StringListContains(accessibleObjectTypes, objDestPolicy.ObjectType) {
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Object type %s is accessble by user %s:%s in orgID %s\n", objDestPolicy.ObjectType, aclUserType, userID, orgID)
					}

					//add object to accessableObjects
					accessibleObjects = append(accessibleObjects, objDestPolicy)
				} else {
					// check if object is public
					if metaData, err := GetObject(objDestPolicy.OrgID, objDestPolicy.ObjectType, objDestPolicy.ObjectID); err != nil {
						return make([]common.ObjectDestinationPolicy, 0), err
					} else {
						if metaData.Public {
							//add object to accessableObjects
							accessibleObjects = append(accessibleObjects, objDestPolicy)
							if trace.IsLogging(logger.DEBUG) {
								trace.Debug("Add public object %s %s %s to result\n", objDestPolicy.OrgID, objDestPolicy.ObjectType, objDestPolicy.ObjectID)
							}
						}
					}
				}
			}
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("AccessableObjects len is: %d\n", len(accessibleObjects))
			}

		}

	}
	// else, accessibleObjects is empty
	return accessibleObjects, nil
}

func GetListOfObjTypes(objMetaList []common.MetaData) []string {
	objTypeList := make([]string, 0)

	for _, objMeta := range objMetaList {
		if !common.StringListContains(objTypeList, objMeta.ObjectType) {
			objTypeList = append(objTypeList, objMeta.ObjectType)
		}
	}

	return objTypeList
}

// swagger:model
type healthReport struct {
	GeneralInfo common.HealthStatusInfo      `json:"general"`
	DBHealth    common.DBHealthStatusInfo    `json:"dbHealth"`
	Usage       *common.UsageInfo            `json:"usage,omitempty"`
	MQTTHealth  *common.MQTTHealthStatusInfo `json:"mqttHealth,omitempty"`
}

// swagger:operation GET /api/v1/health handleHealth
//
// Get health status of the sync service node.
//
// ---
//
// tags:
//   - CSS
//   - ESS
//
// produces:
//   - application/json
//   - text/plain
//
// parameters:
//   - name: details
//     in: query
//     description: Whether or not to include the detailed health status
//     required: false
//     type: boolean
//
// responses:
//   '200':
//     description: Health status
//     schema:
//       type: array
//       items:
//         "$ref": "#/definitions/HealthStatusInfo"
//   '500':
//     description: Failed to send health status.
//     schema:
//       type: string

func handleHealth(writer http.ResponseWriter, request *http.Request) {
	setResponseHeaders(writer)

	detailsString := request.URL.Query().Get("details")
	details := false
	var err error
	if detailsString != "" {
		details, err = strconv.ParseBool(detailsString)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In handleHealth. Include details %t\n", details)
	}

	code, _, _ := security.Authenticate(request)
	if code == security.AuthFailed || code == security.AuthEdgeNode {
		writer.WriteHeader(http.StatusForbidden)
		writer.Write(unauthorizedBytes)
		return
	}

	var registeredESS uint32
	var storedObjects uint32
	if details {
		nodes, err := store.GetNumberOfDestinations()
		if err == nil {
			registeredESS = nodes
		}
		objects, err := store.GetNumberOfStoredObjects()
		if err == nil {
			storedObjects = objects
		}
	}
	common.HealthStatus.UpdateHealthInfo(details, registeredESS, storedObjects)

	report := healthReport{GeneralInfo: common.HealthStatus, DBHealth: common.DBHealth}
	if details {
		report.Usage = &common.HealthUsageInfo
	}
	if common.Configuration.CommunicationProtocol != common.HTTPProtocol {
		report.MQTTHealth = &common.MQTTHealth
	}

	if data, err := json.MarshalIndent(report, "", "  "); err != nil {
		communications.SendErrorResponse(writer, err, "Failed to marshal the health status. Error: ", 0)
	} else {
		writer.Header().Add(contentType, applicationJSON)
		writer.WriteHeader(http.StatusOK)
		if _, err := writer.Write(data); err != nil && log.IsLogging(logger.ERROR) {
			log.Error("Failed to write response body, error: " + err.Error())
		}
	}
}

// Set HTTP cache control headers for http 1.0 and 1.1 clients.
func setResponseHeaders(writer http.ResponseWriter) {
	// Set HTTP cache control headers for http 1.0 and 1.1 clients.
	writer.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	writer.Header().Set("Pragma", "no-cache")

	// Set Strict-Transport-Security headers
	writer.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
}

func validatePathParam(writer http.ResponseWriter, orgID string, objectType string, objectID string, destinationType string, destinationID string) bool {
	var errorMessage string
	orgIDIsValid := true
	objectTypeIsValid := true
	objectIDIsValid := true
	destinationValid := true

	if orgID != "" && !common.IsValidName(orgID) {
		errorMessage = fmt.Sprintf("Organization ID (%s) contains invalid characters", orgID)
		orgIDIsValid = false
	} else if objectType != "" && !common.IsValidName(objectType) {
		errorMessage = fmt.Sprintf("object Type (%s) contains invalid characters", objectType)
		objectTypeIsValid = false
	} else if objectID != "" && !common.IsValidName(objectID) {
		errorMessage = fmt.Sprintf("object ID (%s) contains invalid characters", objectID)
		objectIDIsValid = false
	}

	if destinationType != "" || destinationID != "" {
		destinations := make([]string, 0)
		destination := fmt.Sprintf("%s:%s", destinationType, destinationID)
		destinations = append(destinations, destination)
		if valid, _ := common.ValidateDestinationListInput(destinations); !valid {
			errorMessage = fmt.Sprintf("Destination Type and/or destination ID (%s) contains invalid characters", destination)
			destinationValid = false
		}

	}

	if orgIDIsValid && objectTypeIsValid && objectIDIsValid && destinationValid {
		return true
	}

	writer.WriteHeader(http.StatusBadRequest)
	writer.Header().Add("Content-Type", "Text/Plain")
	buffer := bytes.NewBufferString(errorMessage)
	buffer.WriteString("\n")
	writer.Write(buffer.Bytes())
	return false

}

func validatePathParamForService(writer http.ResponseWriter, serviceOrgID string, serviceName string, propertyName string) bool {
	var errorMessage string
	serviceOrgIDvalid := true
	serviceNameValid := true
	propertyNameValid := true

	if serviceOrgID != "" && !common.IsValidName(serviceOrgID) {
		errorMessage = fmt.Sprintf("Service ID (%s) contains invalid characters", serviceOrgID)
		serviceOrgIDvalid = false
	} else if serviceName != "" && !common.IsValidName(serviceName) {
		errorMessage = fmt.Sprintf("Service name (%s) contains invalid characters", serviceName)
		serviceNameValid = false
	} else if propertyName != "" && !common.IsValidName(propertyName) {
		errorMessage = fmt.Sprintf("Property name (%s) contains invalid characters", propertyName)
		propertyNameValid = false
	}

	if serviceOrgIDvalid && serviceNameValid && propertyNameValid {
		return true
	}

	writer.WriteHeader(http.StatusBadRequest)
	writer.Header().Add("Content-Type", "Text/Plain")
	buffer := bytes.NewBufferString(errorMessage)
	buffer.WriteString("\n")
	writer.Write(buffer.Bytes())
	return false
}

func validatePathParamForSecurity(writer http.ResponseWriter, orgID string, key string, aclUserType string, username string) bool {
	var errorMessage string
	orgIDIsValid := true
	keyIsValid := true
	aclUserTypeIsValid := true
	usernameIsValid := true

	if orgID != "" && !common.IsValidName(orgID) {
		errorMessage = fmt.Sprintf("Organization ID (%s) contains invalid characters", orgID)
		orgIDIsValid = false
	} else if key != "" && !common.IsValidName(key) {
		errorMessage = fmt.Sprintf("Key (destination type/object type) %s contains invalid characters", key)
		keyIsValid = false
	} else if aclUserType != "" && aclUserType != security.ACLUser && aclUserType != security.ACLNode {
		errorMessage = fmt.Sprintf("Invalid acl user type %s in URL. The value should be \"user\" or \"node\"", aclUserType)
		aclUserTypeIsValid = false
	} else if username != "" && !common.IsValidName(username) {
		errorMessage = fmt.Sprintf("Username (%s) contains invalid characters", username)
		usernameIsValid = false
	}

	if orgIDIsValid && keyIsValid && aclUserTypeIsValid && usernameIsValid {
		return true
	}

	writer.WriteHeader(http.StatusBadRequest)
	writer.Header().Add("Content-Type", "Text/Plain")
	buffer := bytes.NewBufferString(errorMessage)
	writer.Write(buffer.Bytes())
	return false
}
