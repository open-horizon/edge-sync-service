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

// PrepareObjectNotifications sends notifications to object’s destinations
// This function should not acquire an object lock (common.ObjectLocks) as the caller has already acquired one.
func PrepareObjectNotifications(metaData common.MetaData) ([]common.NotificationInfo, common.SyncServiceError) {
	destinations, err := Store.GetObjectDestinations(metaData)
	if err == nil {
		err = Store.UpdateObjectDelivering(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if err != nil && log.IsLogging(logger.ERROR) {
			log.Error("Failed to update object's delivery status. Error: " + err.Error())
		}
		return PrepareUpdateNotification(metaData, destinations)
	}
	return nil, nil
}

// PrepareDeleteNotifications prepares the delete notification message
// This function should not acquire an object lock (common.ObjectLocks) as the caller has already acquired one.
func PrepareDeleteNotifications(metaData common.MetaData) ([]common.NotificationInfo, common.SyncServiceError) {
	destinations, err := Store.GetObjectDestinations(metaData)
	if err != nil {
		return nil, err
	}

	return prepareNotifications(common.Delete, metaData, destinations)
}

// PrepareNotificationsForDestinations prepares notification messages for the destinations if necessary
// This function should not acquire an object lock (common.ObjectLocks) as the caller has already acquired one.
func PrepareNotificationsForDestinations(metaData common.MetaData, destinations []common.StoreDestinationStatus, topic string) ([]common.NotificationInfo,
	common.SyncServiceError) {
	dests := make([]common.Destination, 0)
	for _, dest := range destinations {
		if topic != common.Delete || (dest.Status == common.Delivering || dest.Status == common.Delivered || dest.Status == common.Consumed ||
			dest.Status == common.Error) {
			dests = append(dests, dest.Destination)
		}
	}
	return prepareNotifications(topic, metaData, dests)
}

func prepareNotifications(topic string, metaData common.MetaData, destinations []common.Destination) ([]common.NotificationInfo, common.SyncServiceError) {
	result := make([]common.NotificationInfo, 0)

	// Create an initial notification record for each destination
	for _, destination := range destinations {
		notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
			DestOrgID: metaData.DestOrgID, DestID: destination.DestID, DestType: destination.DestType,
			Status: topic, InstanceID: metaData.InstanceID, DataID: metaData.DataID}

		// Store the notification records in storage as part of the object
		if err := Store.UpdateNotificationRecord(notification); err != nil {
			return nil, err
		}

		// Set the DestID in case the object was for destinations of a type or a destinations list
		metaData.DestType = destination.DestType
		metaData.DestID = destination.DestID

		notificationInfo := common.NotificationInfo{NotificationTopic: topic, DestType: metaData.DestType, DestID: metaData.DestID,
			InstanceID: metaData.InstanceID, DataID: metaData.DataID, MetaData: &metaData}
		result = append(result, notificationInfo)
	}
	return result, nil
}

// PrepareUpdateNotification prepares the notification message from object's meta data
// This function should not acquire an object lock (common.ObjectLocks) as the caller has already acquired one.
func PrepareUpdateNotification(metaData common.MetaData, destinations []common.Destination) ([]common.NotificationInfo, common.SyncServiceError) {
	return prepareNotifications(common.Update, metaData, destinations)
}

// PrepareObjectStatusNotification sends an object status message to the other side
// This function should not acquire an object lock (common.ObjectLocks) as the caller has already acquired one.
func PrepareObjectStatusNotification(metaData common.MetaData, status string) ([]common.NotificationInfo, common.SyncServiceError) {
	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: status, InstanceID: metaData.InstanceID, DataID: metaData.DataID}

	// Store the notification records in storage as part of the object
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		return nil, err
	}

	notificationInfo := common.NotificationInfo{NotificationTopic: status, DestType: metaData.OriginType, DestID: metaData.OriginID,
		InstanceID: metaData.InstanceID, DataID: metaData.DataID, MetaData: &metaData}
	return []common.NotificationInfo{notificationInfo}, nil
}

// SendNotifications calls the communication to send the notification messages
func SendNotifications(notifications []common.NotificationInfo) common.SyncServiceError {
	for _, notification := range notifications {
		if err := Comm.SendNotificationMessage(notification.NotificationTopic, notification.DestType, notification.DestID,
			notification.InstanceID, notification.DataID, notification.MetaData); err != nil {
			return &Error{err.Error()}
		}
	}
	return nil
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
			common.ObjectLocks.Lock(lockIndex)
			n, _ := Store.RetrieveNotificationRecord(notification.DestOrgID, notification.ObjectType, notification.ObjectID,
				notification.DestType, notification.DestID)
			if n == nil || n.Status != notification.Status || n.ResendTime != notification.ResendTime {
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			metaData, status, err := Store.RetrieveObjectAndStatus(n.DestOrgID, n.ObjectType, n.ObjectID)
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
				common.ObjectLocks.Unlock(lockIndex)
				return &invalidNotification{message}
			}

			if metaData == nil {
				Store.DeleteNotificationRecords(n.DestOrgID, n.ObjectType, n.ObjectID, "", "")
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			if err := Store.UpdateNotificationResendTime(*n); err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error(err.Error())
				}
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			switch n.Status {
			case common.Getdata:
				if status != common.PartiallyReceived {
					common.ObjectLocks.Unlock(lockIndex)
					continue
				}
				common.ObjectLocks.Unlock(lockIndex)
				Comm.LockDataChunks(lockIndex, metaData)
				offsets := getOffsetsToResend(*n, *metaData)
				for _, offset := range offsets {
					if trace.IsLogging(logger.TRACE) {
						trace.Trace("Resending GetData request for offset %d of %s:%s:%s\n", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
					}
					if err = Comm.GetData(*metaData, offset); err != nil {
						if common.IsNotFound(err) {
							deleteObjectInfo("", "", "", n.DestType, n.DestID, metaData, true)
						}
						break
					}
				}
				Comm.UnlockDataChunks(lockIndex, metaData)

			case common.ReceivedByDestination:
				fallthrough
			case common.Data:
				if dest.DestType == "" {
					common.ObjectLocks.Unlock(lockIndex)
					continue
				}
				// We get here only when an ESS without persistent storage reconnects,
				// and the CSS has a notification with "data" or "received by destination" status.
				// Send update notification for this object.
				n.Status = common.Update
				n.ResendTime = 0
				if err := Store.UpdateNotificationRecord(*n); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Failed to update notification record. Error: " + err.Error())
				}
				common.ObjectLocks.Unlock(lockIndex)
				metaData.DestType = n.DestType
				metaData.DestID = n.DestID
				err = Comm.SendNotificationMessage(common.Update, dest.DestType, dest.DestID, metaData.InstanceID, metaData.DataID, metaData)
			default:
				common.ObjectLocks.Unlock(lockIndex)
				metaData.DestType = n.DestType
				metaData.DestID = n.DestID
				err = Comm.SendNotificationMessage(n.Status, n.DestType, n.DestID, n.InstanceID, n.DataID, metaData)
			}
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error(message)
				}
				return &invalidNotification{message}
			}
		}
	}

	return nil
}

// ResendNotifications resends notications that haven't been acknowledged
func ResendNotifications() common.SyncServiceError {
	if common.Configuration.NodeType == common.ESS && !common.Registered {
		if registerAsNew {
			Comm.RegisterNew()
		} else {
			Comm.Register()
		}
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
	objects, err := Store.GetObjectsToActivate()
	if err != nil && log.IsLogging(logger.ERROR) {
		log.Error("Error in ActivateObjects, failed to retrieve objects. Error: %s\n", err)
	}
	for _, object := range objects {
		if trace.IsLogging(logger.TRACE) {
			trace.Trace("Activating object %s:%s:%s", object.DestOrgID, object.ObjectType, object.ObjectID)
		}
		lockIndex := common.HashStrings(object.DestOrgID, object.ObjectType, object.ObjectID)
		common.ObjectLocks.Lock(lockIndex)

		storedObject, status, err := Store.RetrieveObjectAndStatus(object.DestOrgID, object.ObjectType, object.ObjectID)
		if err != nil || storedObject == nil || status == "" ||
			storedObject.ActivationTime != object.ActivationTime {
			common.ObjectLocks.Unlock(lockIndex)
			continue
		}

		if err := Store.ActivateObject(object.DestOrgID, object.ObjectType, object.ObjectID); err != nil {
			if log.IsLogging(logger.ERROR) {
				log.Error("Error in ActivateObjects. Error: %s\n", err)
			}
			common.ObjectLocks.Unlock(lockIndex)
		} else if status == common.ReadyToSend {
			object.Inactive = false
			notificationsInfo, err := PrepareObjectNotifications(object)
			common.ObjectLocks.Unlock(lockIndex)
			if err == nil {
				if err := SendNotifications(notificationsInfo); err != nil && log.IsLogging(logger.ERROR) {
					log.Error("Error in ActivateObjects: %s\n", err)
				}
			} else if log.IsLogging(logger.ERROR) {
				log.Error("Error in ActivateObjects: %s\n", err)
			}
		} else {
			common.ObjectLocks.Unlock(lockIndex)
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
