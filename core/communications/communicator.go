package communications

import (
	"bytes"
	"net/http"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

// Communicator defines the interface for communications between the CSS and the CSS
type Communicator interface {
	// StartCommunication starts communications
	StartCommunication() common.SyncServiceError

	// StopCommunication stops communications
	StopCommunication() common.SyncServiceError

	// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
	SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64, metaData *common.MetaData) common.SyncServiceError

	// SendFeedbackMessage sends a feedback message from the ESS to the CSS
	SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData) common.SyncServiceError

	// SendErrorMessage sends an error message from the ESS to the CSS
	SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData) common.SyncServiceError

	// Register sends a registration message to be sent by an ESS
	Register() common.SyncServiceError

	// RegisterAck sends a registration acknowledgement message from the CSS
	RegisterAck(destination common.Destination) common.SyncServiceError

	// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
	GetData(metaData common.MetaData, offset int64) common.SyncServiceError

	// SendData sends data from the CSS to the ESS or from the ESS to the CSS
	SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError

	// ResendObjects requests to resend all the relevant objects
	ResendObjects() common.SyncServiceError

	// SendAckResendObjects sends ack to resend objects request
	SendAckResendObjects(destination common.Destination) common.SyncServiceError

	// UpdateOrganization adds or updates an organization
	UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError

	// DeleteOrganization removes an organization
	DeleteOrganization(orgID string) common.SyncServiceError
}

// Error is the error struct used by the communications code
type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// Store is a reference to the Storage being used
var Store storage.Storage

// Comm is the selected communications struct
var Comm Communicator

// SendErrorResponse common code to send HTTP error codes
func SendErrorResponse(writer http.ResponseWriter, err error, message string, statusCode int) {
	writer.Header().Add("Content-Type", "Text/Plain")

	if statusCode == 0 {
		switch err.(type) {
		case *common.InvalidRequest:
			statusCode = http.StatusBadRequest
		case *storage.Error:
			statusCode = http.StatusInternalServerError
		case *storage.NotConnected:
			statusCode = http.StatusServiceUnavailable
		default:
			statusCode = http.StatusInternalServerError
		}
	}
	writer.WriteHeader(statusCode)

	buffer := bytes.NewBufferString(message)
	if err != nil {
		buffer.WriteString(err.Error())
	}
	buffer.WriteString("\n")
	writer.Write(buffer.Bytes())
}
