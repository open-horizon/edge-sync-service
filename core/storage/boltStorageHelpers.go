package storage

import (
	"encoding/json"

	bolt "github.com/etcd-io/bbolt"
	"github.com/open-horizon/edge-sync-service/common"
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

func (store *BoltStorage) viewObjectHelper(orgID string, objectType string, objectID string, retrieve func(boltObject) common.SyncServiceError) common.SyncServiceError {
	id := createObjectCollectionID(orgID, objectType, objectID)
	err := store.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(objectsBucket).Get([]byte(id))
		if encoded == nil {
			return notFound
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

func (store *BoltStorage) lock() {
	<-store.lockChannel
}

func (store *BoltStorage) unLock() {
	store.lockChannel <- 1
}
