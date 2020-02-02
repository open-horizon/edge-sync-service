package communications

import (
	"os"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func TestNotifications(t *testing.T) {

	dest1 := common.Destination{DestOrgID: "myorg", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	dest2 := common.Destination{DestOrgID: "myorg", DestType: "device", DestID: "dev2", Communication: common.MQTTProtocol}
	dest3 := common.Destination{DestOrgID: "myorg", DestType: "device2", DestID: "dev1", Communication: common.MQTTProtocol}
	dest4 := common.Destination{DestOrgID: "myorg", DestType: "device2", DestID: "dev2", Communication: common.MQTTProtocol}

	destinations := []common.Destination{dest1, dest2, dest3, dest4}

	tests := []struct {
		metaData common.MetaData
		dests    []common.Destination
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device", Link: "abc",
			OriginID: "device2", OriginType: "dev2"},
			[]common.Destination{dest1}},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg", DestType: "device", Link: "abc",
			OriginID: "device2", OriginType: "dev2"},
			[]common.Destination{dest1, dest2}},
	}

	//Store = &storage.InMemoryStorage{}
	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
	boltStore := &storage.BoltStorage{}
	Store = boltStore
	if err := Store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	boltStore.Cleanup(true)

	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	for _, row := range tests {

		// Update notification
		notificationsInfo, err := PrepareUpdateNotification(row.metaData, row.dests)
		if err != nil {
			t.Errorf("Failed to send update notification. Error: %s", err.Error())
		}
		err = SendNotifications(notificationsInfo)
		if err != nil {
			t.Errorf("Failed to send update notification. Error: %s", err.Error())
		}
		for _, dest := range row.dests {
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				dest.DestType, dest.DestID)
			if err != nil && !storage.IsNotFound(err) {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
				} else if notification.Status != common.Update {
					t.Errorf("Wrong notification status: %s instead of update (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
				}
			}
		}

		// Consumed notification is sent to the origin
		notificationsInfo, err = PrepareObjectStatusNotification(row.metaData, common.Consumed)
		if err != nil {
			t.Errorf("Failed to send consumed notification. Error: %s", err.Error())
		}
		err = SendNotifications(notificationsInfo)
		if err != nil {
			t.Errorf("Failed to send consumed notification. Error: %s", err.Error())
		}
		for _, dest := range row.dests {
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				dest.DestType, dest.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
				} else if notification.Status != common.Update {
					t.Errorf("Wrong notification status: %s instead of update (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
				}
			}
		}
		notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err != nil {
			t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else {
			if notification == nil {
				t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
			} else if notification.Status != common.Consumed {
				t.Errorf("Wrong notification status: %s instead of consumed", notification.Status)
			}
		}

		// Deleted notification is sent to the origin
		notificationsInfo, err = PrepareObjectStatusNotification(row.metaData, common.Deleted)
		if err != nil {
			t.Errorf("Failed to send deleted notification. Error: %s", err.Error())
		}
		err = SendNotifications(notificationsInfo)
		if err != nil {
			t.Errorf("Failed to send deleted notification. Error: %s", err.Error())
		}
		for _, dest := range row.dests {
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				dest.DestType, dest.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
				} else {
					if notification.Status != common.Update {
						t.Errorf("Wrong notification status: %s instead of update (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
					}
				}
			}
		}
		notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err != nil {
			t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else {
			if notification == nil {
				t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
			} else if notification.Status != common.Deleted {
				t.Errorf("Wrong notification status: %s instead of deleted", notification.Status)
			}
		}

		for _, dest := range row.dests {
			if err := Store.DeleteNotificationRecords(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID, dest.DestType, dest.DestID); err != nil {
				t.Errorf("Failed to send delete notifications. Error: %s", err.Error())
			}
		}

		// The tests below will not work with InMemory storage

		// Send update notifications by retrieving destinations from the object
		common.Configuration.NodeType = common.CSS

		for _, dest := range destinations {
			if err := Store.StoreDestination(dest); err != nil {
				t.Errorf("Failed to store destination. Error: %s", err.Error())
			}
		}

		if _, err := Store.StoreObject(row.metaData, []byte("data"), common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object. Error: %s", err.Error())
		}
		notificationsInfo, err = PrepareObjectNotifications(row.metaData)
		if err != nil {
			t.Errorf("Failed to send update notification. Error: %s", err.Error())
		}

		for _, dest := range row.dests {
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				dest.DestType, dest.DestID)
			if err != nil && !storage.IsNotFound(err) {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s) dest %s:%s", row.metaData.ObjectID, dest.DestType, dest.DestID)
				} else {
					if notification.Status != common.Update {
						t.Errorf("Wrong notification status: %s instead of update (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
					}
				}
			}
		}

		notificationsInfo, err = PrepareDeleteNotifications(row.metaData)
		if err != nil {
			t.Errorf("Failed to send delete notification. Error: %s", err.Error())
		}

		for _, dest := range row.dests {
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				dest.DestType, dest.DestID)
			if err != nil && !storage.IsNotFound(err) {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
				} else {
					if notification.Status != common.Delete {
						t.Errorf("Wrong notification status: %s instead of update (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
					}
				}
			}
		}
	}
}

func TestActivateObjects(t *testing.T) {

	activationTime1 := time.Now().Add(time.Second * 2).UTC().Format(time.RFC3339)
	activationTime2 := time.Now().Add(time.Second * 5).UTC().Format(time.RFC3339)

	tests := []struct {
		metaData common.MetaData
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "notmyorg", DestID: "dev1", DestType: "device", Link: "abc",
			Inactive: true, ActivationTime: activationTime1}},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "notmyorg", DestID: "dev1", DestType: "device", Link: "abc",
			Inactive: true, ActivationTime: activationTime2}},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "notmyorg", DestID: "dev1", DestType: "device", Link: "abc",
			Inactive: false}},
	}

	//Store = &storage.InMemoryStorage{}
	common.Configuration.NodeType = common.CSS
	boltStore := &storage.BoltStorage{}
	boltStore.Cleanup(true)
	Store = boltStore
	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
	if err := Store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	dest := common.Destination{DestOrgID: "notmyorg", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := handleRegisterNew(dest, false); err != nil {
		t.Errorf("handleRegisterNew failed. Error: %s\n", err.Error())
	}

	for _, test := range tests {
		if _, err := Store.StoreObject(test.metaData, nil, common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}
	objects, err := Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveObjects returned %d objects instead of 1\n", len(objects))
	}

	time.Sleep(4 * time.Second)

	ActivateObjects()

	objects, err = Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 2 {
		t.Errorf("RetrieveObjects returned %d objects instead of 2\n", len(objects))
		t.Errorf("    The time is now: %s", time.Now().UTC().Format(time.RFC3339))
		for _, object := range objects {
			t.Errorf("    Retrieved object:  %#v", object)
		}
	}

	time.Sleep(4 * time.Second)

	ActivateObjects()

	objects, err = Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 3 {
		t.Errorf("RetrieveObjects returned %d objects instead of 3\n", len(objects))
	}
}
