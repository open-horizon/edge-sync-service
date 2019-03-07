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
	tests := []struct {
		metaData common.MetaData
		dests    []common.Destination
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device", Link: "abc",
			OriginID: "device2", OriginType: "dev2"},
			[]common.Destination{dest1, dest2, dest3, dest4}},
	}

	//Store = &storage.InMemoryStorage{}
	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
	boltStore := &storage.BoltStorage{}
	boltStore.Cleanup()
	Store = boltStore
	if err := Store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}
	for _, row := range tests {

		// Update notification
		if err := SendNotification(row.metaData, row.dests); err != nil {
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
		if err := SendObjectStatus(row.metaData, common.Consumed); err != nil {
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
				} else if notification.Status != common.Consumed && dest.DestType == row.metaData.OriginType && dest.DestID == row.metaData.OriginID {
					t.Errorf("Wrong notification status: %s instead of consumed (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
				}
			}
		}

		// Deleted notification is sent to the origin
		if err := SendObjectStatus(row.metaData, common.Deleted); err != nil {
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
				} else if notification.Status != common.Deleted && dest.DestType == row.metaData.OriginType && dest.DestID == row.metaData.OriginID {
					t.Errorf("Wrong notification status: %s instead of deleted (DestType:DestID = %s)", notification.Status, dest.DestType+":"+dest.DestID)
				}
			}
		}

		// SendObjectNotifications and SendDeleteNotifications can't be tested with inMemoryStorage (it doesn't return destinations in RetrieveDestinations)
	}
}

func TestActivateObjects(t *testing.T) {

	activationTime1 := time.Now().Format(time.RFC3339)
	activationTime2 := time.Now().Add(time.Second * 1).Format(time.RFC3339)

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
	boltStore := &storage.BoltStorage{}
	boltStore.Cleanup()
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
	for _, test := range tests {
		if err := Store.StoreObject(test.metaData, nil, common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}
	objects, err := Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveObjects returned %d objects instead of 1\n", len(objects))
	}

	ActivateObjects()
	select {
	case <-time.After(1 * time.Second):
		objects, err = Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
		if err != nil {
			t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
		} else if len(objects) != 2 {
			t.Errorf("RetrieveObjects returned %d objects instead of 2\n", len(objects))
		}
	}

	select {
	case <-time.After(1 * time.Second):
		ActivateObjects()
		objects, err := Store.RetrieveObjects("notmyorg", "device", "dev1", common.ResendAll)
		if err != nil {
			t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
		} else if len(objects) != 3 {
			t.Errorf("RetrieveObjects returned %d objects instead of 3\n", len(objects))
		}
	}
}
