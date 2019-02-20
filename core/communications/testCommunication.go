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
	destID string, instanceID int64, metaData *common.MetaData) common.SyncServiceError {
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
