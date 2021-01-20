package storage

import (
	"encoding/json"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataURI"
	bolt "go.etcd.io/bbolt"
)

func (store *BoltStorage) retrieveObjectsHelper(retrieve func(boltObject)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(objectsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var object boltObject
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			retrieve(object)
		}
		return nil
	})

	return err
}

func (store *BoltStorage) updateObjectHelper(orgID string, objectType string, objectID string,
	update func(boltObject) (boltObject, common.SyncServiceError)) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	err := store.db.Update(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(objectsBucket).Get([]byte(id))
		if encoded == nil {
			return notFound
		}

		var err error
		var object boltObject
		if err = json.Unmarshal(encoded, &object); err != nil {
			return err
		}

		object, err = update(object)
		if err != nil {
			return err
		}
		encoded, err = json.Marshal(object)
		if err != nil {
			return err
		}
		err = tx.Bucket(objectsBucket).Put([]byte(id), []byte(encoded))
		return err
	})
	return err
}

func (store *BoltStorage) updateObjectsHelper(update func(boltObject) (*boltObject, common.SyncServiceError)) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(objectsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var object boltObject
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			updatedObject, err := update(object)
			if err != nil {
				return err
			}
			if updatedObject == nil {
				continue
			}
			encoded, err := json.Marshal(*updatedObject)
			if err != nil {
				return err
			}
			err = tx.Bucket(objectsBucket).Put(key, []byte(encoded))
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) deleteObjectsHelper(match func(boltObject) bool) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(objectsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var object boltObject
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			if match(object) {
				if object.DataPath != "" {
					if err := dataURI.DeleteStoredData(object.DataPath); err != nil {
						return err
					}
					object.DataPath = ""
				}
				if err := tx.Bucket(objectsBucket).Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) deleteObjectsAndNotificationsHelper(match func(boltObject) bool) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		objectCursor := tx.Bucket(objectsBucket).Cursor()

		for objectKey, objectValue := objectCursor.First(); objectKey != nil; objectKey, objectValue = objectCursor.Next() {
			var object boltObject
			if err := json.Unmarshal(objectValue, &object); err != nil {
				return err
			}
			if match(object) {
				if object.DataPath != "" {
					if err := dataURI.DeleteStoredData(object.DataPath); err != nil {
						return err
					}
					object.DataPath = ""
				}
				if err := tx.Bucket(objectsBucket).Delete(objectKey); err != nil {
					return err
				}

				notifyCursor := tx.Bucket(notificationsBucket).Cursor()
				for notifyKey, notifyValue := notifyCursor.First(); notifyKey != nil; notifyKey, notifyValue = notifyCursor.Next() {
					var notification common.Notification
					if err := json.Unmarshal(notifyValue, &notification); err != nil {
						return err
					}
					if notification.DestOrgID == object.Meta.DestOrgID && notification.ObjectType == object.Meta.ObjectType &&
						notification.ObjectID == object.Meta.ObjectID {
						if err := tx.Bucket(notificationsBucket).Delete(notifyKey); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) viewObjectHelper(orgID string, objectType string, objectID string, retrieve func(boltObject) common.SyncServiceError) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(objectsBucket).Get([]byte(id))
		if encoded == nil {
			return &common.NotFound{}
		}

		var object boltObject
		if err := json.Unmarshal(encoded, &object); err != nil {
			return err
		}

		err := retrieve(object)
		return err
	})
	return err
}

func (store *BoltStorage) updateWebhookHelper(objectType string,
	update func(hooks []string) []string) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(webhooksBucket).Get([]byte(objectType))
		var hooks []string
		var err error
		if encoded != nil {
			if err := json.Unmarshal(encoded, &hooks); err != nil {
				return err
			}
		}

		hooks = update(hooks)
		if hooks == nil {
			// No need to write back
			return nil
		}
		encoded, err = json.Marshal(hooks)
		if err != nil {
			return err
		}
		err = tx.Bucket(webhooksBucket).Put([]byte(objectType), []byte(encoded))
		return err
	})
	return err
}

func (store *BoltStorage) retrieveNotificationsHelper(retrieve func(common.Notification)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(notificationsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var notification common.Notification
			if err := json.Unmarshal(value, &notification); err != nil {
				return err
			}
			retrieve(notification)
		}
		return nil
	})

	return err
}

func (store *BoltStorage) deleteNotificationsHelper(match func(common.Notification) bool) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(notificationsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var notification common.Notification
			if err := json.Unmarshal(value, &notification); err != nil {
				return err
			}
			if match(notification) {
				if err := tx.Bucket(notificationsBucket).Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) updateNotificationHelper(notification common.Notification,
	update func(*common.Notification) (*common.Notification, common.SyncServiceError)) common.SyncServiceError {
	id := getNotificationCollectionID(&notification)
	return store.updateNotificationHelperWithID(id, update)
}

func (store *BoltStorage) updateNotificationHelperWithID(id string,
	update func(*common.Notification) (*common.Notification, common.SyncServiceError)) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		var n *common.Notification
		encoded := tx.Bucket(notificationsBucket).Get([]byte(id))
		if encoded != nil {
			var decoded common.Notification
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				return err
			}
			n = &decoded
		}

		notification, err := update(n)
		if err != nil {
			return err
		}
		encoded, err = json.Marshal(notification)
		if err != nil {
			return err
		}

		err = tx.Bucket(notificationsBucket).Put([]byte(id), []byte(encoded))
		return err
	})
	return err
}

func (store *BoltStorage) viewNotificationHelper(orgID string, objectType string, objectID string, destType string,
	destID string, retrieve func(common.Notification) common.SyncServiceError) common.SyncServiceError {
	id := createNotificationCollectionID(orgID, objectType, objectID, destType, destID)
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(notificationsBucket).Get([]byte(id))
		if encoded == nil {
			return notFound
		}

		var notification common.Notification
		if err := json.Unmarshal(encoded, &notification); err != nil {
			return err
		}

		if err := retrieve(notification); err != nil {
			return err
		}

		return nil
	})
	return err
}

func (store *BoltStorage) retrieveDestinationsHelper(retrieve func(boltDestination)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(destinationsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var dest boltDestination
			if err := json.Unmarshal(value, &dest); err != nil {
				return err
			}
			retrieve(dest)
		}
		return nil
	})

	return err
}

func (store *BoltStorage) updateDestinationHelper(id string, update func(boltDestination) boltDestination) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(destinationsBucket).Get([]byte(id))
		if encoded == nil {
			return notFound
		}

		var err error
		var dest boltDestination
		if err = json.Unmarshal(encoded, &dest); err != nil {
			return err
		}

		dest = update(dest)

		encoded, err = json.Marshal(dest)
		if err != nil {
			return err
		}
		err = tx.Bucket(destinationsBucket).Put([]byte(id), []byte(encoded))
		return err
	})
	return err
}

func (store *BoltStorage) deleteDestinationsHelper(match func(boltDestination) bool) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(destinationsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var dest boltDestination
			if err := json.Unmarshal(value, &dest); err != nil {
				return err
			}
			if match(dest) {
				if err := tx.Bucket(destinationsBucket).Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) viewDestinationHelper(orgID string, destType string, destID string, retrieve func(boltDestination) common.SyncServiceError) common.SyncServiceError {
	id := createDestinationCollectionID(orgID, destType, destID)
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(destinationsBucket).Get([]byte(id))
		if encoded == nil {
			return notFound
		}

		var dest boltDestination
		if err := json.Unmarshal(encoded, &dest); err != nil {
			return err
		}

		err := retrieve(dest)
		return err
	})
	return err
}

func (store *BoltStorage) retrieveMessagingGroupHelper(retrieve func(boltMessagingGroup)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(messagingGroupsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var mg boltMessagingGroup
			if err := json.Unmarshal(value, &mg); err != nil {
				return err
			}
			retrieve(mg)
		}
		return nil
	})

	return err
}

func (store *BoltStorage) retrieveOrganizationsHelper(retrieve func(common.StoredOrganization)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(organizationsBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var org common.StoredOrganization
			if err := json.Unmarshal(value, &org); err != nil {
				return err
			}
			retrieve(org)
		}
		return nil
	})

	return err
}

func (store *BoltStorage) updateACLHelper(aclType string, orgID string, key string, update func(acl boltACL) (*boltACL, bool)) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		id := orgID + ":" + aclType + ":" + key

		encoded := tx.Bucket(aclBucket).Get([]byte(id))
		var acl boltACL
		var err error
		if encoded != nil {
			if err := json.Unmarshal(encoded, &acl); err != nil {
				return err
			}
		} else {
			acl = boltACL{make([]common.ACLentry, 0), orgID, aclType, key}
		}

		updatedACL, delete := update(acl)
		if updatedACL == nil && !delete {
			// No need to write back
			return nil
		}
		if delete {
			err = tx.Bucket(aclBucket).Delete([]byte(id))
		} else {
			encoded, err = json.Marshal(updatedACL)
			if err != nil {
				return err
			}
			err = tx.Bucket(aclBucket).Put([]byte(id), []byte(encoded))
		}
		return err
	})
	return err
}

func (store *BoltStorage) deleteACLsHelper(match func(acl boltACL) bool) common.SyncServiceError {
	err := store.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(aclBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var acl boltACL
			if err := json.Unmarshal(value, &acl); err != nil {
				return err
			}
			if match(acl) {
				if err := tx.Bucket(aclBucket).Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return err
}

func (store *BoltStorage) retrieveACLHelper(retrieve func(boltACL)) common.SyncServiceError {
	err := store.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(aclBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var acl boltACL
			if err := json.Unmarshal(value, &acl); err != nil {
				return err
			}
			retrieve(acl)
		}
		return nil
	})

	return err
}

func createObjectDestinationPolicy(object boltObject) common.ObjectDestinationPolicy {
	destinationList := make([]common.DestinationsStatus, len(object.Destinations))
	for index, destination := range object.Destinations {
		destinationList[index] = common.DestinationsStatus{
			DestType: destination.Destination.DestType, DestID: destination.Destination.DestID,
			Status: destination.Status, Message: destination.Message,
		}
	}
	return common.ObjectDestinationPolicy{
		OrgID: object.Meta.DestOrgID, ObjectType: object.Meta.ObjectType, ObjectID: object.Meta.ObjectID,
		DestinationPolicy: object.Meta.DestinationPolicy, Destinations: destinationList,
	}
}

func (store *BoltStorage) lock() {
	<-store.lockChannel
}

func (store *BoltStorage) unLock() {
	store.lockChannel <- 1
}
