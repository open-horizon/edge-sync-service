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
func PrepareObjectNotifications(metaData common.MetaData) ([]common.NotificationInfo, common.SyncServiceError) {
	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.Lock(lockIndex)

	destinations, err := Store.GetObjectDestinations(metaData)
	if err == nil {
		err = Store.UpdateObjectDelivering(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if err != nil && log.IsLogging(logger.ERROR) {
		log.Error("Failed to update object's delivery status. Error: %s", err.Error())
		}
		common.ObjectLocks.Unlock(lockIndex)
		return PrepareUpdateNotification(metaData, destinations)
	}
	common.ObjectLocks.Unlock(lockIndex)
	return nil, nil
}

// PrepareDeleteNotifications prepares the delete notification message
func PrepareDeleteNotifications(metaData common.MetaData) ([]common.NotificationInfo, common.SyncServiceError) {
	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.Lock(lockIndex)
	destinations, err := Store.GetObjectDestinations(metaData)
	if err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return nil, err
	}
	common.ObjectLocks.Unlock(lockIndex)
	return prepareNotifications(common.Delete, metaData, destinations)
}

// PrepareNotificationsForDestinations prepares notification messages for the destinations if necessary
func PrepareNotificationsForDestinations(metaData common.MetaData, destinations []common.StoreDestinationStatus, topic string) ([]common.NotificationInfo,
	common.SyncServiceError) {
	dests := make([]common.Destination, 0)

	for _, dest := range destinations {
		lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		common.ObjectLocks.Lock(lockIndex)
		// If topic is delete (delete destination), and destination status is pending => notification will NOT be prepared for this destination
		if topic != common.Delete || (dest.Status == common.Delivering || dest.Status == common.Delivered || dest.Status == common.Consumed ||
			dest.Status == common.Error) {
			dests = append(dests, dest.Destination)
		}
		common.ObjectLocks.Unlock(lockIndex)
	}
	return prepareNotifications(topic, metaData, dests)
}

func prepareNotifications(topic string, metaData common.MetaData, destinations []common.Destination) ([]common.NotificationInfo, common.SyncServiceError) {
	result := make([]common.NotificationInfo, 0)

	// Create an initial notification record for each destination
	for _, destination := range destinations {
		lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		common.ObjectLocks.Lock(lockIndex)
		notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
			DestOrgID: metaData.DestOrgID, DestID: destination.DestID, DestType: destination.DestType,
			Status: topic, InstanceID: metaData.InstanceID, DataID: metaData.DataID}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In notification.go, prepareNotifications update notification for %s %s %s %s with status %s\n", metaData.ObjectType, metaData.ObjectID, destination.DestType, destination.DestID, topic)
		}
		// Store the notification records in storage as part of the object
		if err := Store.UpdateNotificationRecord(notification); err != nil {
			common.ObjectLocks.Unlock(lockIndex)
			return nil, err
		}

		if trace.IsLogging(logger.DEBUG) {
			trace.Debug("In notification.go, prepareNotifications, update notification for %s %s %s %s with status %s is done\n", metaData.ObjectType, metaData.ObjectID, destination.DestType, destination.DestID, topic)
		}

		// Set the DestID in case the object was for destinations of a type or a destinations list
		metaData.DestType = destination.DestType
		metaData.DestID = destination.DestID

		notificationInfo := common.NotificationInfo{NotificationTopic: topic, DestType: metaData.DestType, DestID: metaData.DestID,
			InstanceID: metaData.InstanceID, DataID: metaData.DataID, MetaData: &metaData}
		result = append(result, notificationInfo)
		common.ObjectLocks.Unlock(lockIndex)
	}
	return result, nil
}

// PrepareUpdateNotification prepares the notification message from object's meta data
func PrepareUpdateNotification(metaData common.MetaData, destinations []common.Destination) ([]common.NotificationInfo, common.SyncServiceError) {
	return prepareNotifications(common.Update, metaData, destinations)
}

// PrepareObjectStatusNotification sends an object status message to the other side
func PrepareObjectStatusNotification(metaData common.MetaData, status string) ([]common.NotificationInfo, common.SyncServiceError) {
	lockIndex := common.HashStrings(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
	common.ObjectLocks.Lock(lockIndex)
	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.OriginID, DestType: metaData.OriginType,
		Status: status, InstanceID: metaData.InstanceID, DataID: metaData.DataID}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In notification.go, PrepareObjectStatusNotification, update notification for %s %s %s %s with status %s\n", metaData.ObjectType, metaData.ObjectID, notification.DestType, notification.DestID, notification.Status)
	}
	// Store the notification records in storage as part of the object
	if err := Store.UpdateNotificationRecord(notification); err != nil {
		common.ObjectLocks.Unlock(lockIndex)
		return nil, err
	}
	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In notification.go, PrepareObjectStatusNotification, update notification for %s %s %s %s with status %s is done\n", metaData.ObjectType, metaData.ObjectID, notification.DestType, notification.DestID, notification.Status)
	}

	notificationInfo := common.NotificationInfo{NotificationTopic: status, DestType: metaData.OriginType, DestID: metaData.OriginID,
		InstanceID: metaData.InstanceID, DataID: metaData.DataID, MetaData: &metaData}

	common.ObjectLocks.Unlock(lockIndex)
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
	// Update, Received, GetData, Error, Delete, Deleted
	notifications, err := Store.RetrieveNotifications(dest.DestOrgID, dest.DestType, dest.DestID, resendReceivedObjects)
	if err != nil {
		message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
		if log.IsLogging(logger.ERROR) {
			log.Error("%s", message)
		}
		return &invalidNotification{message}
	}

	if trace.IsLogging(logger.DEBUG) {
		trace.Debug("In notification.go, resendNotificationsForDestination: get %d notification to resend\n", len(notifications))
	}

	if len(notifications) > 0 {
		for _, notification := range notifications {
			// Retrieve the notification in case it was changed since the call to RetrieveNotifications
			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In notification.go, Retrieve the notification in case it was changed since the call to RetrieveNotifications\n")
			}
			lockIndex := common.HashStrings(notification.DestOrgID, notification.ObjectType, notification.ObjectID)
			common.ObjectLocks.Lock(lockIndex)
			n, _ := Store.RetrieveNotificationRecord(notification.DestOrgID, notification.ObjectType, notification.ObjectID,
				notification.DestType, notification.DestID)
			if n == nil || n.Status != notification.Status || n.ResendTime != notification.ResendTime {
				if trace.IsLogging(logger.DEBUG) {
					if n == nil {
						trace.Debug("In notification.go, resendNotificationsForDestination: n is nil, continue")
					} else if n.Status != notification.Status {
						trace.Debug("In notification.go, resendNotificationsForDestination: n.Status: %s, notificaiton.Status: %s, continue", n.Status, notification.Status)
					} else {
						trace.Debug("In notification.go, resendNotificationsForDestination: n.ResendTime: %d, notificaiton.ResendTime: %d, continue", n.ResendTime, notification.ResendTime)
					}
				}
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In notification.go, resendNotificationsForDestination: n.status is %s, retrive object and status...", n.Status)
			}

			metaData, status, err := Store.RetrieveObjectAndStatus(n.DestOrgID, n.ObjectType, n.ObjectID)
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error("%s", message)
				}
				common.ObjectLocks.Unlock(lockIndex)
				return &invalidNotification{message}
			}

			if metaData == nil {
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In notification.go, resendNotificationsForDestination: metaData is nil, delete notification record and continue")
				}
				Store.DeleteNotificationRecords(n.DestOrgID, n.ObjectType, n.ObjectID, "", "")
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In notification.go, resendNotificationsForDestination: update notification resendtime")
			}
			if err := Store.UpdateNotificationResendTime(*n); err != nil {
				if log.IsLogging(logger.ERROR) {
			log.Error("%s", err.Error())
				}
				common.ObjectLocks.Unlock(lockIndex)
				continue
			}

			if trace.IsLogging(logger.DEBUG) {
				trace.Debug("In notification.go, resendNotificationsForDestination: metaData.InstanceID: %d, notification.InstanceID: %d", metaData.InstanceID, n.InstanceID)
			}

			switch n.Status {
			case common.Getdata:
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In notification.go, notification getdata status for destination, need to resend object %s %s to destination %s %s", n.ObjectType, n.ObjectID, n.DestType, n.DestID)
				}

				if status != common.PartiallyReceived {
					common.ObjectLocks.Unlock(lockIndex)
					continue
				}
				common.ObjectLocks.Unlock(lockIndex)

				nc, err := Store.RetrieveNotificationRecord(notification.DestOrgID, notification.ObjectType, notification.ObjectID,
					notification.DestType, notification.DestID)
				if err == nil && nc != nil && nc.Status == notification.Status && nc.InstanceID == notification.InstanceID {
					Comm.LockDataChunks(lockIndex, metaData)
					offsets := getOffsetsToResend(*n, *metaData)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("len(offsets) to resend %d for %s:%s:%s\n", len(offsets), n.DestOrgID, n.ObjectType, n.ObjectID)
					}
					for _, offset := range offsets {
						if trace.IsLogging(logger.DEBUG) {
							trace.Debug("Resending GetData request for offset %d of %s:%s:%s\n", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
						}
						if err = Comm.GetData(*metaData, offset); err != nil {
							if common.IsNotFound(err) {
								if log.IsLogging(logger.ERROR) {
									log.Error("Resending GetData, get notFound error for offset %d of %s:%s:%s, deleting object Info...", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
								}
								deleteObjectInfo("", "", "", n.DestType, n.DestID, metaData, true)
							}
							break
						}
					}
					Comm.UnlockDataChunks(lockIndex, metaData)
				} else {
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Retrieved notification is nil or with different instanceID or status")
					}
				}

				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In notification.go, notification getdata status for destination, resend object %s %s to destination %s %s done", n.ObjectType, n.ObjectID, n.DestType, n.DestID)
				}
			case common.ReceivedByDestination:
				fallthrough
			case common.Updated:
				if common.Configuration.NodeType == common.CSS {
					if dest.DestType == "" {
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In notification.go, notification %s status for destination with no persistent storage, need to resend object %s %s to destination %s %s\n", n.Status, n.ObjectType, n.ObjectID, n.DestType, n.DestID)
					}

					// We get here only when an ESS without persistent storage reconnects,
					// and the CSS has a notification with "updated" or "received by destination" status.
					// Send update notification for this object (then notification status will be changed to: updatePending)
					n.Status = common.Update
					n.ResendTime = 0
					if err := Store.UpdateNotificationRecord(*n); err != nil && log.IsLogging(logger.ERROR) {
			log.Error("Failed to update notification record. Error: %s", err.Error())
					}
					common.ObjectLocks.Unlock(lockIndex)
					metaData.DestType = n.DestType
					metaData.DestID = n.DestID
					err = Comm.SendNotificationMessage(common.Update, dest.DestType, dest.DestID, metaData.InstanceID, metaData.DataID, metaData)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In notification.go, done with resend objects for notification with data status, metaData.DestType: %s, metaData.DestID: %s\n", metaData.DestType, metaData.DestID)
					}
				} else {
					common.ObjectLocks.Unlock(lockIndex)
				}
			case common.Data:
				if dest.DestType == "" {
					common.ObjectLocks.Unlock(lockIndex)
					continue
				}
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In notification.go, notification data status for destination, need to resend object %s %s to destination %s %s\n", n.ObjectType, n.ObjectID, n.DestType, n.DestID)
				}

				if common.Configuration.NodeType == common.ESS {
					// ESS with a data status, is in progress of sending data to CSS
					common.ObjectLocks.Unlock(lockIndex)
					Comm.LockDataChunks(lockIndex, metaData)
					offsets := getOffsetsToResend(*n, *metaData)
					for _, offset := range offsets {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Resending pushData request for offset %d of %s:%s:%s\n", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
						}
						if err = Comm.PushData(metaData, offset); err != nil {
							if common.IsNotFound(err) {
								if log.IsLogging(logger.ERROR) {
									log.Error("Resending Data, get notFound error for offset %d of %s:%s:%s, deleting object Info...", offset, n.DestOrgID, n.ObjectType, n.ObjectID)
								}
								deleteObjectInfo("", "", "", n.DestType, n.DestID, metaData, true)
							}
							break
						}
					}
					Comm.UnlockDataChunks(lockIndex, metaData)
				}

			case common.Error:
				// resend only when error notification instance ID == metadata instanceID
				if metaData.InstanceID == n.InstanceID {
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In notification.go, notification error status for destination, need to resend object %s %s to destination %s %s\n", n.ObjectType, n.ObjectID, n.DestType, n.DestID)
					}
					n.Status = common.Update
					n.ResendTime = 0
					if err := Store.UpdateNotificationRecord(*n); err != nil && log.IsLogging(logger.ERROR) {
						log.Error("Failed to update notification record. Error: " + err.Error())
					}
					common.ObjectLocks.Unlock(lockIndex)
					metaData.DestType = n.DestType
					metaData.DestID = n.DestID
					err = Comm.SendNotificationMessage(common.Update, n.DestType, n.DestID, metaData.InstanceID, metaData.DataID, metaData)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In notification.go, done with resend objects for notification with error, metaData.DestType: %s, metaData.DestID: %s", metaData.DestType, metaData.DestID)
					}
				} else {
					common.ObjectLocks.Unlock(lockIndex)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("In notification.go, error notification.InstanceID(%d) != metadata.InstanceID(%d), ignore resend object %s %s to destination %s %s", n.InstanceID, metaData.InstanceID, n.ObjectType, n.ObjectID, n.DestType, n.DestID)
					}
				}

			default:
				common.ObjectLocks.Unlock(lockIndex)
				metaData.DestType = n.DestType
				metaData.DestID = n.DestID
				if trace.IsLogging(logger.DEBUG) {
					trace.Debug("In notification.go, notification %s status for destination, need to resend object %s %s to destination %s %s", n.Status, n.ObjectType, n.ObjectID, n.DestType, n.DestID)
				}
				err = Comm.SendNotificationMessage(n.Status, n.DestType, n.DestID, n.InstanceID, n.DataID, metaData)
			}
			if err != nil {
				message := fmt.Sprintf("Error in resendNotificationsForDestination. Error: %s\n", err)
				if log.IsLogging(logger.ERROR) {
					log.Error("%s", message)
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
			common.ObjectLocks.Unlock(lockIndex)
			object.Inactive = false
			notificationsInfo, err := PrepareObjectNotifications(object)
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
			request, _ := http.NewRequest("POST", url, bytes.NewReader(body))
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
			err = response.Body.Close()
			if err != nil {
				if log.IsLogging(logger.ERROR) {
					log.Error("Error in callWebhooks, failed to close response body")
				}
			}
		}
	}
}
