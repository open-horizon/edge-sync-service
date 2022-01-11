package communications

import (
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

// Wrapper is the struct for a wrapper around the MQTT and HTTP communications between the CSS and ESS
type Wrapper struct {
	httpComm *HTTP
	mqttComm *MQTT
}

// NewWrapper creates a new Wrapper struct
func NewWrapper(httpComm *HTTP, mqttComm *MQTT) *Wrapper {
	return &Wrapper{httpComm, mqttComm}
}

// StartCommunication starts communications
func (communication *Wrapper) StartCommunication() common.SyncServiceError {
	return nil
}

// StopCommunication stops communications
func (communication *Wrapper) StopCommunication() common.SyncServiceError {
	var err1, err2 error
	if communication.httpComm != nil {
		err1 = communication.httpComm.StopCommunication()
	}
	if communication.mqttComm != nil {
		err2 = communication.mqttComm.StopCommunication()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (communication *Wrapper) selectCommunicator(protocol string, orgID string, destType string, destID string) (Communicator, common.SyncServiceError) {
	var comm Communicator
	var err common.SyncServiceError

	if protocol == "" {
		if common.Configuration.NodeType == common.CSS && common.Configuration.CommunicationProtocol != common.HybridMQTT &&
			common.Configuration.CommunicationProtocol != common.HybridWIoTP {
			protocol = common.Configuration.CommunicationProtocol
		} else {
			protocol, err = Store.RetrieveDestinationProtocol(orgID, destType, destID)
			if err != nil {
				return nil, err
			}
		}
	}
	switch protocol {
	case common.HTTPProtocol:
		comm = communication.httpComm
	case common.MQTTProtocol:
		fallthrough
	case common.WIoTP:
		comm = communication.mqttComm
	default:
		err = &Error{"Failed to select protocol for communication"}
	}

	return comm, err
}

// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
func (communication *Wrapper) SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64, dataID int64,
	metaData *common.MetaData) common.SyncServiceError {
	comm, err := communication.selectCommunicator("", metaData.DestOrgID, destType, destID)
	if err != nil {
		return err
	}
	return comm.SendNotificationMessage(notificationTopic, destType, destID, instanceID, dataID, metaData)
}

// SendFeedbackMessage sends a feedback message from the ESS to the CSS or from the CSS to the ESS
func (communication *Wrapper) SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err != nil {
		return err
	}
	return comm.SendFeedbackMessage(code, retryInterval, reason, metaData, sendToOrigin)
}

// SendErrorMessage sends an error message from the ESS to the CSS or from the CSS to the ESS
func (communication *Wrapper) SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err != nil {
		return err
	}
	return comm.SendErrorMessage(err, metaData, sendToOrigin)
}

// Register sends a registration message to be sent by an ESS
func (communication *Wrapper) Register() common.SyncServiceError {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err != nil {
		return err
	}
	return comm.Register()
}

// RegisterAck sends a registration acknowledgement message from the CSS
func (communication *Wrapper) RegisterAck(destination common.Destination) common.SyncServiceError {
	comm, err := communication.selectCommunicator(destination.Communication, "", "", "")
	if err != nil {
		return err
	}
	return comm.RegisterAck(destination)
}

// RegisterAsNew send a notification from a CSS to a ESS that the ESS has to send a registerNew message in order
// to register
func (communication *Wrapper) RegisterAsNew(destination common.Destination) common.SyncServiceError {
	comm, err := communication.selectCommunicator(destination.Communication, "", "", "")
	if err != nil {
		return err
	}
	return comm.RegisterAsNew(destination)
}

// RegisterNew sends a new registration message to be sent by an ESS
func (communication *Wrapper) RegisterNew() common.SyncServiceError {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err != nil {
		return err
	}
	return comm.RegisterNew()
}

// Unregister ESS
// TODO: implement Unregister() method for mqttCommunication
func (communication *Wrapper) Unregister() common.SyncServiceError {
	if common.Configuration.CommunicationProtocol == "http" {
		comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
		if err != nil {
			return err
		}
		return comm.Unregister()
	}

	return &Error{"ESS unregister only support in http communication protocol\n"}
}

// HandleRegAck handles a registration acknowledgement message from the CSS
func (communication *Wrapper) HandleRegAck() {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err == nil {
		comm.HandleRegAck()
	}
}

// SendPing sends a ping message from ESS to CSS
func (communication *Wrapper) SendPing() common.SyncServiceError {
	comm, err := communication.selectCommunicator(common.Configuration.CommunicationProtocol, "", "", "")
	if err != nil {
		return err
	}
	return comm.SendPing()
}

// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
func (communication *Wrapper) GetData(metaData common.MetaData, offset int64) common.SyncServiceError {
	comm, err := communication.selectCommunicator("", metaData.DestOrgID, metaData.OriginType, metaData.OriginID)
	if err != nil {
		return err
	}
	return comm.GetData(metaData, offset)
}

// PushData uploade data to from ESS to CSS
func (communication *Wrapper) PushData(metaData *common.MetaData, offset int64) common.SyncServiceError {
	comm, err := communication.selectCommunicator("", metaData.DestOrgID, metaData.OriginType, metaData.OriginID)
	if err != nil {
		return err
	}
	return comm.PushData(metaData, offset)
}

// SendData sends data from the CSS to the ESS or from the ESS to the CSS
func (communication *Wrapper) SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError {
	comm, err := communication.selectCommunicator("", orgID, destType, destID)
	if err != nil {
		return err
	}
	return comm.SendData(orgID, destType, destID, message, chunked)
}

// ResendObjects requests to resend all the relevant objects
func (communication *Wrapper) ResendObjects() common.SyncServiceError {
	comm, err := communication.selectCommunicator("", "", "", "")
	if err != nil {
		return err
	}
	return comm.ResendObjects()
}

// SendAckResendObjects sends ack to resend objects request
func (communication *Wrapper) SendAckResendObjects(destination common.Destination) common.SyncServiceError {
	comm, err := communication.selectCommunicator(destination.Communication, destination.DestOrgID,
		destination.DestType, destination.DestID)
	if err != nil {
		return err
	}
	return comm.SendAckResendObjects(destination)
}

// UpdateOrganization adds or updates an organization
func (communication *Wrapper) UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS || common.Configuration.CommunicationProtocol == common.HTTPProtocol {
		return nil
	}
	return communication.mqttComm.UpdateOrganization(org, timestamp)
}

// DeleteOrganization removes an organization
func (communication *Wrapper) DeleteOrganization(orgID string) common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS || common.Configuration.CommunicationProtocol == common.HTTPProtocol {
		return nil
	}
	return communication.mqttComm.DeleteOrganization(orgID)
}

// LockDataChunks locks one of the data chunks locks
func (communication *Wrapper) LockDataChunks(index uint32, metadata *common.MetaData) {
	var comm Communicator
	var err error

	if metadata == nil {
		comm = communication.mqttComm
	} else {
		comm, err = communication.selectCommunicator("", metadata.DestOrgID, metadata.OriginType, metadata.OriginID)
		if err != nil {
			comm = communication.mqttComm
		}
	}
	if comm != nil {
		comm.LockDataChunks(index, metadata)
	}
}

// UnlockDataChunks unlocks one of the data chunks locks
func (communication *Wrapper) UnlockDataChunks(index uint32, metadata *common.MetaData) {
	var comm Communicator
	var err error

	if metadata == nil {
		comm = communication.mqttComm
	} else {
		comm, err = communication.selectCommunicator("", metadata.DestOrgID, metadata.OriginType, metadata.OriginID)
		if err != nil {
			comm = communication.mqttComm
		}
	}
	if comm != nil {
		comm.UnlockDataChunks(index, metadata)
	}
}
