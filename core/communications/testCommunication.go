package communications

import (
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

// TestComm is a communicator used for unit testing
type TestComm struct {
}

// StartCommunication starts communications
func (communication *TestComm) StartCommunication() common.SyncServiceError {
	return nil
}

// StopCommunication stops communications
func (communication *TestComm) StopCommunication() common.SyncServiceError {
	return nil
}

// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
func (communication *TestComm) SendNotificationMessage(notificationTopic string, destType string,
	destID string, instanceID int64, dataID int64, metaData *common.MetaData) common.SyncServiceError {
	return nil
}

// Register sends a registration message to be sent by an ESS
func (communication *TestComm) Register() common.SyncServiceError {
	return nil
}

// RegisterAck sends a registration acknowledgement message from the CSS
func (communication *TestComm) RegisterAck(destination common.Destination) common.SyncServiceError {
	return nil
}

// HandleRegAck handles a registration acknowledgement message from the CSS
func (communication *TestComm) HandleRegAck() {}

// RegisterAsNew send a notification from a CSS to a ESS that the ESS has to send a registerNew message in order
// to register
func (communication *TestComm) RegisterAsNew(destination common.Destination) common.SyncServiceError {
	return nil
}

// RegisterNew sends a new registration message to be sent by an ESS
func (communication *TestComm) RegisterNew() common.SyncServiceError {
	return nil
}

// Unregister ESS
func (communication *TestComm) Unregister() common.SyncServiceError {
	return nil
}

// SendPing sends a ping message from ESS to CSS
func (communication *TestComm) SendPing() common.SyncServiceError {
	return nil
}

// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
func (communication *TestComm) GetData(metaData common.MetaData, offset int64) common.SyncServiceError {
	err := updateGetDataNotification(metaData, metaData.OriginType, metaData.OriginID, offset)
	return err
}

// SendData sends data from the CSS to the ESS or from the ESS to the CSS
func (communication *TestComm) SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError {
	return nil
}

// ResendObjects requests to resend all the relevant objects
func (communication *TestComm) ResendObjects() common.SyncServiceError {
	return nil
}

// SendAckResendObjects sends ack to resend objects request
func (communication *TestComm) SendAckResendObjects(destination common.Destination) common.SyncServiceError {
	return nil
}

// UpdateOrganization adds or updates an organization
func (communication *TestComm) UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError {
	return nil
}

// DeleteOrganization removes an organization
func (communication *TestComm) DeleteOrganization(orgID string) common.SyncServiceError {
	return nil
}

// LockDataChunks locks one of the data chunks locks
func (communication *TestComm) LockDataChunks(index uint32, metadata *common.MetaData) {
	// Noop on HTTP
}

// UnlockDataChunks unlocks one of the data chunks locks
func (communication *TestComm) UnlockDataChunks(index uint32, metadata *common.MetaData) {
	// Noop on HTTP
}

// SendFeedbackMessage sends a feedback message from the ESS to the CSS or from the CSS to the ESS
func (communication *TestComm) SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	return nil
}

// SendErrorMessage sends an error message from the ESS to the CSS or from the CSS to the ESS
func (communication *TestComm) SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	return nil
}
