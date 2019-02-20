package communications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type invalidNotification struct {
	message string
}

func (e *invalidNotification) Error() string {
	return e.message
}

// SendObjectNotifications sends notifications to object’s destinations
func SendObjectNotifications(metaData common.MetaData) common.SyncServiceError {
	destinations, err := Store.GetObjectDestinations(metaData)
	if err == nil {
		err = Store.UpdateObjectDelivering(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if err != nil && log.IsLogging(logger.ERROR) {
			log.Error("Failed to update object's delivery status. Error: " + err.Error())
		}
		return SendNotification(metaData, destinations)
	}
	return nil
}

// SendDeleteNotifications prepares the delete notification message
func SendDeleteNotifications(metaData common.MetaData) common.SyncServiceError {
	destinations, err := Store.GetObjectDestinations(metaData)
	if err != nil {
		return err
	}

	return sendNotifications(common.Delete, metaData, destinations)
}

func sendNotifications(topic string, metaData common.MetaData, destinations []common.Destination) common.SyncServiceError {
	var sendError common.SyncServiceError

	// Create an initial notification record for each destination
	for _, destination := range destinations {
		notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
			DestOrgID: metaData.DestOrgID, DestID: destination.DestID, DestType: destination.DestType,
			Status: topic, InstanceID: metaData.InstanceID}

		// Store the notification records in storage as part of the object
		if err := Store.UpdateNotificationRecord(notification); err != nil {
			return err
		}

		// Set the DestID in case the object was for destinations of a type or a destinations list
		metaData.DestType = destination.DestType
		metaData.DestID = destination.DestID

		// Send a notification message via the communication module to each destination
		if err := Comm.SendNotificationMessage(topic, metaData.DestType, metaData.DestID, metaData.InstanceID,
			&metaData); err != nil {
			sendError = err
		}
	}
	return sendError
}

// SendNotification prepares the notification message from object's meta data
func SendNotification(metaData common.MetaData, destinations []common.Destination) common.SyncServiceError {
	return sendNotifications(common.Update, metaData, destinations)
}

// SendObjectStatus sends an object status message to the other side
func SendObjectStatus(metaData common.MetaData, status string) common.SyncServiceError {
	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: status, InstanceID: metaData.InstanceID}

	// Store the notification records in storage as part of the object
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		return err
	}

	return Comm.SendNotificationMessage(status, metaData.OriginType, metaData.OriginID, metaData.InstanceID, &metaData)
}

func resendNotificationsForDestination(dest common.Destination, resendReceivedObjects bool) common.SyncServiceError {
	notifications, err := Store.RetrieveNotifications(dest.DestOrgID, dest.DestType, dest.DestID, resendReceivedObjects)
	if err != nil {
		message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
		if log.IsLogging(logger.ERROR) {
			log.Error(message)
		}
		return &invalidNotification{message}
	}

	if len(notifications) > 0 {
		for _, notification := range notifications {
			// Retrieve the notification in case it was changed since the call to RetrieveNotifications
			lockIndex := common.HashStrings(notification.DestOrgID, notification.ObjectType, notification.ObjectID)
			lockObject(lockIndex)
			n, _ := Store.RetrieveNotificationRecord(notification.DestOrgID, notification.ObjectType, notification.ObjectID,
				notification.DestType, notification.DestID)
			if n == nil || n.Status != notification.Status || n.ResendTime != notification.ResendTime {
				unlockObject(lockIndex)
				continue
			}

			metaData, err := Store.RetrieveObject(n.DestOrgID, n.ObjectType, n.ObjectID)
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
				unlockObject(lockIndex)
				return &invalidNotification{message}
			}

			if metaData == nil {
				Store.DeleteNotificationRecords(n.DestOrgID, n.ObjectType, n.ObjectID, "", "")
				unlockObject(lockIndex)
				continue
			}

			if err := Store.UpdateNotificationResendTime(*n); err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error(err.Error())
				}
				unlockObject(lockIndex)
				continue
			}

			switch n.Status {
			case common.Getdata:
				offsets := getOffsetsToResend(*n, *metaData)
				for _, offset := range offsets {
					if trace.IsLogging(logger.TRACE) {
						trace.Trace("Resending GetData request for offset %d of %s:%s:%s\n", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
					}
					if err = Comm.GetData(*metaData, offset); err != nil {
						break
					}
				}

			case common.ReceivedByDestination:
				fallthrough
			case common.Data:
				if dest.DestType == "" {
					unlockObject(lockIndex)
					continue
				}
				// We get here only when an ESS without persistent storage reconnects,
				// and the CSS has a notification with "data" or "received by destination" status.
				// Send update notification for this object.
				n.Status = common.Update
				if err := Store.UpdateNotificationRecord(*n); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to update notification record. Error: " + err.Error())
				}
				metaData.DestType = n.DestType
				metaData.DestID = n.DestID
				err = Comm.SendNotificationMessage(common.Update, dest.DestType, dest.DestID, metaData.InstanceID, metaData)
			default:
				metaData.DestType = n.DestType
				metaData.DestID = n.DestID
				err = Comm.SendNotificationMessage(n.Status, n.DestType, n.DestID, n.InstanceID, metaData)
			}
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
				unlockObject(lockIndex)
				return &invalidNotification{message}
			}
			if trace.IsLogging(logger.TRACE) {
				trace.Trace("Resent notification\n")
			}
			unlockObject(lockIndex)
		}
	}

	return nil
}

// ResendNotifications resends notications that haven't been acknowledged
func ResendNotifications() common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS && !common.Registered {
		Comm.Register()
	}

	if common.Configuration.NodeType == common.ESS && !common.ResendAcked {
		Comm.ResendObjects()
	}

	if leader.CheckIfLeader() {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("About to resend notifications.")
		}
		return resendNotificationsForDestination(common.Destination{}, false)
	}
	return nil
}

// ActivateObjects looks for objects that are ready to be activated, marks them as active, and sends
// object notifications to their destinations
func ActivateObjects() {
	objects, statuses, err := Store.GetObjectsToActivate()
	if err != nil && log.IsLogging(logger.ERROR) {
		log.Error("Error in ActivateObjects, failed to retrieve objects. Error: %s\n", err)
	}
	for i, object := range objects {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Activating object %s:%s:%s", object.DestOrgID, object.ObjectType, object.ObjectID)
		}
		if err := Store.ActivateObject(object.DestOrgID, object.ObjectType, object.ObjectID); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in ActivateObjects. Error: %s\n", err)
			}
		} else if statuses[i] == common.ReadyToSend {
			object.Inactive = false
			if err := SendObjectNotifications(object); err != nil && log.IsLogging(logger.ERROR) {
				log.Error("Error in mongoStorage.checkObjects: %s\n", err)
			}
		}
	}
}

// ResendObjects requests to resend all the relevant objects
func ResendObjects() common.SyncServiceError {
	common.ResendAcked = false
	return Comm.ResendObjects()
}

func callWebhooks(metaData *common.MetaData) {
	if webhooks, err := Store.RetrieveWebhooks(metaData.DestOrgID, metaData.ObjectType); err == nil {
		body, err := json.MarshalIndent(metaData, "", "  ")
		if err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in callWebhooks, failed to marshal meta data: %s\n", err)
			}
			return
		}
		for _, url := range webhooks {
			request, err := http.NewRequest("POST", url, bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			request.Header.Add("Content-Type", "Application/JSON")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error("Error in callWebhooks, failed to post meta data to %s: %s\n", url, err)
				}
				continue
			}
			if response.StatusCode != http.StatusOK &&
				response.StatusCode != http.StatusNoContent &&
				log.IsLogging(logger.ERROR) {
				log.Error("Error in callWebhooks: received status: %d for %s\n", response.StatusCode, url)
			}
			response.Body.Close()
		}
	}
}
