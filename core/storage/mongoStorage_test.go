package storage

import (
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestMongoStorageObjects(t *testing.T) {
	testStorageObjects(mongo, t)
}

func TestMongoStorageObjectActivation(t *testing.T) {
	testStorageObjectActivation(mongo, t)
}

func TestMongoStorageObjectExpiration(t *testing.T) {
	common.Configuration.StorageMaintenanceInterval = 1
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	dest1 := common.Destination{DestOrgID: "1myorg1", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	expirationTime1 := time.Now().Add(time.Second * 1).Format(time.RFC3339)
	expirationTime2 := time.Now().Add(time.Second * 3).Format(time.RFC3339)

	tests := []struct {
		metaData common.MetaData
		status   string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device"},
			common.ReadyToSend},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Expiration: expirationTime1}, common.ReadyToSend},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Expiration: expirationTime2}, common.ReadyToSend},
	}

	for _, test := range tests {
		// Insert
		if err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	objects, err := store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 3 {
		t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 5\n", len(objects))
	}

	select {
	case <-time.After(2 * time.Second):
		store.PerformMaintenance()

		objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
		if err != nil {
			t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
		} else if len(objects) != 2 {
			t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 2\n", len(objects))
		}
	}

	select {
	case <-time.After(4 * time.Second):
		store.PerformMaintenance()

		objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
		if err != nil {
			t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
		} else if len(objects) != 1 {
			t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 1\n", len(objects))
		} else if objects[0].ObjectID != "1" {
			t.Errorf("Incorrect object: %s instead of objectId=1\n", objects[0].ObjectID)
		}
	}
}

func TestMongoStorageObjectData(t *testing.T) {
	testStorageObjectData(mongo, t)
}

func TestMongoStorageOrgDeleteObjects(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	dest1 := common.Destination{DestOrgID: "zzzmyorg1", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	tests := []struct {
		metaData common.MetaData
		status   string
		data     []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "zzzmyorg1", DestID: "dev1", DestType: "device"},
			common.ReadyToSend, []byte("abcdefghijklmnopqrstuvwxyz")},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "zzzmyorg1", DestID: "dev1", DestType: "device",
			Inactive: true}, common.CompletelyReceived, []byte("abcdefghijklmnopqrstuvwxyz")},
		{common.MetaData{ObjectID: "3", ObjectType: "type2", DestOrgID: "zzzmyorg1", DestID: "dev1", DestType: "device", NoData: true},
			common.ReadyToSend, nil},
		{common.MetaData{ObjectID: "4", ObjectType: "type2", DestOrgID: "zzzmyorg2", DestID: "dev1", DestType: "device"},
			common.CompletelyReceived, []byte("abcdefghijklmnopqrstuvwxyz")},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "zzzmyorg1", DestID: "dev1", DestType: "device"},
			common.ObjReceived, []byte("abcdefghijklmnopqrstuvwxyz")},
	}

	for _, test := range tests {
		// Insert
		if err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	objects, err := store.RetrieveUpdatedObjects(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType, false)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveUpdatedObjects returned wrong number of objects: %d instead of 1\n", len(objects))
	} else if objects[0].ObjectID != "2" {
		t.Errorf("RetrieveUpdatedObjects returned wrong object: %s instead of object ID = 2\n", objects[0].ObjectID)
	}

	objects, err = store.RetrieveUpdatedObjects(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType, true)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 2 {
		t.Errorf("RetrieveUpdatedObjects returned wrong number of objects: %d instead of 2\n", len(objects))
	}

	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 2 {
		t.Errorf("RetrieveObjects returned wrong number of objects: %d instead of 2\n", len(objects))
	}

	// DeleteOrganization deletes all the objects of this org
	if err := store.DeleteOrganization(tests[0].metaData.DestOrgID); err != nil {
		t.Errorf("DeleteOrganization failed. Error: %s\n", err.Error())
	}
	objects, err = store.RetrieveUpdatedObjects(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType, true)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 0 {
		t.Errorf("RetrieveUpdatedObjects returned objects after the organization has been deleted\n")
	}
	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 0 {
		t.Errorf("RetrieveObjects returned objects after the organization has been deleted\n")
	}
	objects, err = store.RetrieveUpdatedObjects(tests[3].metaData.DestOrgID, tests[3].metaData.ObjectType, false)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveUpdatedObjects returned wrong number of objects: %d instead of 1\n", len(objects))
	} else if objects[0].ObjectID != "4" {
		t.Errorf("RetrieveUpdatedObjects returned wrong object: %s instead of object ID = 4\n", objects[0].ObjectID)
	}
	objects, err = store.RetrieveUpdatedObjects(tests[3].metaData.DestOrgID, tests[3].metaData.ObjectType, true)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveUpdatedObjects returned wrong number of objects: %d instead of 1\n", len(objects))
	} else if objects[0].ObjectID != "4" {
		t.Errorf("RetrieveUpdatedObjects returned wrong object: %s instead of object ID = 4\n", objects[0].ObjectID)
	}
}

func TestMongoStorageDestinations(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		dest common.Destination
	}{
		{common.Destination{DestOrgID: "myorg123", DestID: "1", DestType: "device", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "myorg123", DestID: "1", DestType: "device2", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "myorg123", DestID: "2", DestType: "device2", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "myorg2", DestID: "1", DestType: "device", Communication: common.HTTPProtocol}},
	}

	for _, test := range tests {
		if err := store.StoreDestination(test.dest); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}

		if exists, _ := store.DestinationExists(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); !exists {
			t.Errorf("Stored destination doesn't exist\n")
		}

		if protocol, err := store.RetrieveDestinationProtocol(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); err != nil {
			t.Errorf("RetrieveDestinationProtocol failed. Error: %s\n", err.Error())
		} else if protocol != test.dest.Communication {
			t.Errorf("RetrieveDestinationProtocol returned incorrect protocol %s instead of %s\n", protocol, test.dest.Communication)
		}

		if dest, err := store.RetrieveDestination(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); err != nil {
			t.Errorf("RetrieveDestinationProtocol failed. Error: %s\n", err.Error())
		} else if dest.Communication != test.dest.Communication {
			t.Errorf("Incorrect protocol %s instead of %s\n", dest.Communication, test.dest.Communication)
		} else if dest.DestOrgID != test.dest.DestOrgID {
			t.Errorf("Incorrect org %s instead of %s\n", dest.DestOrgID, test.dest.DestOrgID)
		} else if dest.DestType != test.dest.DestType {
			t.Errorf("Incorrect type %s instead of %s\n", dest.Communication, test.dest.Communication)
		} else if dest.DestID != test.dest.DestID {
			t.Errorf("Incorrect ID %s instead of %s\n", dest.Communication, test.dest.Communication)
		}
	}

	if dests, err := store.RetrieveDestinations("myorg123", "device"); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 1 {
		t.Errorf("Wrong number of destinations: %d instead of 1\n", len(dests))
	} else if dests[0] != tests[0].dest {
		t.Errorf("Wrong destination\n")
	}

	if dests, err := store.RetrieveDestinations("myorg123", "device2"); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 2 {
		t.Errorf("Wrong number of destinations: %d instead of 2\n", len(dests))
	}

	for _, test := range tests {
		if err := store.DeleteDestination(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); err != nil {
			t.Errorf("DeleteDestination failed. Error: %s\n", err.Error())
		}

		if exists, _ := store.DestinationExists(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); exists {
			t.Errorf("Deleted destination exists\n")
		}
	}

	if dests, err := store.RetrieveDestinations("myorg123", ""); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 0 {
		t.Errorf("Wrong number of destinations: %d instead of 0\n", len(dests))
	}
	if dests, err := store.RetrieveDestinations("myorg2", ""); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 0 {
		t.Errorf("Wrong number of destinations: %d instead of 0\n", len(dests))
	}

	// DeleteOrganization deletes all the destinations of this org
	for _, test := range tests {
		if err := store.StoreDestination(test.dest); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}
	}
	if err := store.DeleteOrganization("myorg123"); err != nil {
		t.Errorf("DeleteOrganization failed. Error: %s\n", err.Error())
	}
	if dests, err := store.RetrieveDestinations("myorg123", ""); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 0 {
		t.Errorf("RetrieveDestinations returned destinations after the organization has been deleted")
	}
	if dests, err := store.RetrieveDestinations("myorg2", ""); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 1 {
		t.Errorf("Wrong number of destinations: %d instead of 1\n", len(dests))
	}
}

func TestMongoStorageNotifications(t *testing.T) {
	testStorageNotifications(mongo, t)
}

func TestMongoStorageOrgDeleteNotifications(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		n common.Notification
	}{
		{common.Notification{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device",
			Status: common.Update, InstanceID: 5}},
		{common.Notification{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg123", DestID: "2", DestType: "device",
			Status: common.Update, InstanceID: 5}},
		{common.Notification{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device",
			Status: common.Getdata}},
		{common.Notification{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device",
			Status: common.Data}},
		{common.Notification{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device",
			Status: common.AckConsumed, InstanceID: 9}},
		{common.Notification{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device",
			Status: common.UpdatePending}},
		{common.Notification{ObjectID: "6", ObjectType: "type1", DestOrgID: "myorg456", DestID: "1", DestType: "device2",
			Status: common.DeletePending}},
	}

	// DeleteOrganization deletes all the notifications of this org
	for _, test := range tests {
		if err := store.UpdateNotificationRecord(test.n); err != nil {
			t.Errorf("UpdateNotificationRecord failed. Error: %s\n", err.Error())
		}
	}
	if err := store.DeleteOrganization("myorg123"); err != nil {
		t.Errorf("DeleteOrganization failed. Error: %s\n", err.Error())
	}
	if notifications, err := store.RetrieveNotifications(tests[0].n.DestOrgID, tests[0].n.DestType, tests[0].n.DestID, false); err != nil {
		t.Errorf("RetrieveNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 0 {
		t.Errorf("RetrieveNotifications returned notifications after the organization has been deleted\n")
	}
	if notifications, err := store.RetrievePendingNotifications(tests[5].n.DestOrgID, "", ""); err != nil {
		t.Errorf("RetrievePendingNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 0 {
		t.Errorf("RetrievePendingNotifications returned notifications after the organization has been deleted\n")
	}
	if notifications, err := store.RetrievePendingNotifications(tests[6].n.DestOrgID, "", ""); err != nil {
		t.Errorf("RetrievePendingNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 1 {
		t.Errorf("RetrievePendingNotifications returned incorrect %d instead of 1\n", len(notifications))
	}
}

func TestMongoStorageMessagingGroups(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		orgID     string
		groupName string
	}{
		{"org1", "mg1"},
		{"org2", "mg1"},
		{"org3", "mg3"},
	}

	for _, test := range tests {
		if err := store.StoreOrgToMessagingGroup(test.orgID, test.groupName); err != nil {
			t.Errorf("StoreOrgToMessagingGroup failed. Error: %s\n", err.Error())
		}
		if groupName, err := store.RetrieveMessagingGroup(test.orgID); err != nil {
			t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
		} else if groupName != test.groupName {
			t.Errorf("RetrieveMessagingGroup returned incorrect group name: %s instead of %s\n", groupName, test.groupName)
		}
	}

	if groupName, err := store.RetrieveMessagingGroup("lalala"); err != nil {
		t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
	} else if groupName != "" {
		t.Errorf("RetrieveMessagingGroup returned group name (%s) for non-existing org\n", groupName)
	}

	if err := store.StoreOrgToMessagingGroup("org1", "mg2"); err != nil {
		t.Errorf("StoreOrgToMessagingGroup failed. Error: %s\n", err.Error())
	}
	if groupName, err := store.RetrieveMessagingGroup("org1"); err != nil {
		t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
	} else if groupName != "mg2" {
		t.Errorf("RetrieveMessagingGroup returned incorrect group name: %s instead of mg2\n", groupName)
	}

	for _, test := range tests {
		if err := store.DeleteOrgToMessagingGroup(test.orgID); err != nil {
			t.Errorf("StoreOrgToMessagingGroup failed. Error: %s\n", err.Error())
		}
		if groupName, err := store.RetrieveMessagingGroup(test.orgID); err != nil {
			t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
		} else if groupName != "" {
			t.Errorf("RetrieveMessagingGroup returned group name (%s) for non-existing org\n", groupName)
		}
	}

	// DeleteOrganization deletes all the mappings of this org
	for _, test := range tests {
		if err := store.StoreOrgToMessagingGroup(test.orgID, test.groupName); err != nil {
			t.Errorf("StoreOrgToMessagingGroup failed. Error: %s\n", err.Error())
		}
	}
	if err := store.DeleteOrganization("org1"); err != nil {
		t.Errorf("DeleteOrganization failed. Error: %s\n", err.Error())
	}
	if groupName, err := store.RetrieveMessagingGroup("org1"); err != nil {
		t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
	} else if groupName != "" {
		t.Errorf("RetrieveMessagingGroup returned group name (%s) after the organization has been deleted\n", groupName)
	}
	if groupName, err := store.RetrieveMessagingGroup("org2"); err != nil {
		t.Errorf("RetrieveMessagingGroup failed. Error: %s\n", err.Error())
	} else if groupName != "mg1" {
		t.Errorf("RetrieveMessagingGroup returned incorrect group name: %s instead of mg1\n", groupName)
	}
}

func TestMongoStorageObjectDestinations(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	dest1 := common.Destination{DestOrgID: "org444", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	dest2 := common.Destination{DestOrgID: "org444", DestType: "device", DestID: "dev2", Communication: common.MQTTProtocol}
	destArray := []string{"device:dev2", "device:dev1"}

	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}
	if err := store.StoreDestination(dest2); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	tests := []struct {
		metaData common.MetaData
		status   string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "org444", DestType: "device",
			Inactive: true}, common.NotReadyToSend},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device",
			Inactive: true}, common.NotReadyToSend},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device",
			NoData: true}, common.ReadyToSend},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "org444",
			DestinationsList: destArray, NoData: true}, common.ReadyToSend},
	}

	for _, test := range tests {
		// Delete the object first
		if err := store.DeleteStoredObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		// Insert
		if err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		// Check destinations
		if dests, err := store.GetObjectDestinations(test.metaData); err != nil {
			t.Errorf("GetObjectDestinations failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if test.metaData.DestType != "" && test.metaData.DestID != "" {
				if len(dests) != 1 {
					t.Errorf("GetObjectDestinations returned wrong number of destinations: %d instead of 1 (objectID = %s).\n",
						len(dests), test.metaData.ObjectID)
				} else if dests[0] != dest1 {
					t.Errorf("GetObjectDestinations returned wrong destination (objectID = %s).\n",
						test.metaData.ObjectID)
				}
			} else {
				if len(dests) != 2 {
					t.Errorf("GetObjectDestinations returned wrong number of destinations: %d instead of 2 (objectID = %s).\n",
						len(dests), test.metaData.ObjectID)
				} else {
					if (dests[0] != dest2 && dests[0] != dest1) || (dests[1] != dest1 && dests[1] != dest2) {
						t.Errorf("GetObjectDestinations returned wrong destination (objectID = %s).\n",
							test.metaData.ObjectID)
					}
				}

			}
		}
		// Check destinations list
		if dests, err := store.GetObjectDestinationsList(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("GetObjectDestinationsList failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if test.metaData.DestType != "" && test.metaData.DestID != "" {
				if len(dests) != 1 {
					t.Errorf("GetObjectDestinationsList returned wrong number of destinations: %d instead of 1 (objectID = %s).\n",
						len(dests), test.metaData.ObjectID)
				} else if dests[0].Destination != dest1 {
					t.Errorf("GetObjectDestinations returned wrong destination (objectID = %s).\n",
						test.metaData.ObjectID)
				} else if dests[0].Status != common.Pending {
					t.Errorf("GetObjectDestinations returned wrong status: %s instead of Pending (objectID = %s).\n", dests[0].Status,
						test.metaData.ObjectID)
				} else if dests[0].Message != "" {
					t.Errorf("GetObjectDestinations returned wrong message: %s instead of empty message (objectID = %s).\n", dests[0].Message,
						test.metaData.ObjectID)
				}
			} else {
				if len(dests) != 2 {
					t.Errorf("GetObjectDestinationsList returned wrong number of destinations: %d instead of 2 (objectID = %s).\n",
						len(dests), test.metaData.ObjectID)
				} else {
					if (dests[0].Destination != dest2 && dests[0].Destination != dest1) || (dests[1].Destination != dest1 && dests[1].Destination != dest2) {
						t.Errorf("GetObjectDestinationsList returned wrong destination (objectID = %s).\n",
							test.metaData.ObjectID)
					} else {
						if dests[0].Status != common.Pending {
							t.Errorf("GetObjectDestinations returned wrong status: %s instead of Pending (objectID = %s).\n", dests[0].Status,
								test.metaData.ObjectID)
						}
						if dests[1].Status != common.Pending {
							t.Errorf("GetObjectDestinations returned wrong status: %s instead of Pending (objectID = %s).\n", dests[1].Status,
								test.metaData.ObjectID)
						}
						if dests[0].Message != "" {
							t.Errorf("GetObjectDestinations returned wrong message: %s instead of empty message (objectID = %s).\n", dests[0].Message,
								test.metaData.ObjectID)
						}
						if dests[1].Message != "" {
							t.Errorf("GetObjectDestinations returned wrong message: %s instead of empty message (objectID = %s).\n", dests[1].Message,
								test.metaData.ObjectID)
						}
					}
				}
			}
		}

		// Change status to Delivering for all the destinations
		if err := store.UpdateObjectDelivering(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("UpdateObjectDelivering failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if dests, err := store.GetObjectDestinationsList(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("GetObjectDestinationsList failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(dests) == 0 {
				t.Errorf("GetObjectDestinationsList returned no destinations (objectID = %s).\n", test.metaData.ObjectID)
			}
			for _, d := range dests {
				if d.Status != common.Delivering {
					t.Errorf("GetObjectDestinations returned wrong status: %s instead of Delivering (objectID = %s).\n", d.Status,
						test.metaData.ObjectID)
				}
			}
		}

		// Change status to Delivered for dest1
		if err := store.UpdateObjectDeliveryStatus(common.Delivered, "", test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
			dest1.DestType, dest1.DestID); err != nil {
			t.Errorf("UpdateObjectDeliveryStatus failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if dests, err := store.GetObjectDestinationsList(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("GetObjectDestinationsList failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(dests) == 0 {
				t.Errorf("GetObjectDestinationsList returned no destinations (objectID = %s).\n", test.metaData.ObjectID)
			}
			for _, d := range dests {
				if d.Status != common.Delivered && d.Destination != dest2 {
					t.Errorf("GetObjectDestinations returned wrong status: %s instead of Delivered (objectID = %s).\n", d.Status,
						test.metaData.ObjectID)
				}
			}
		}

		// Change status to Error for dest1
		if err := store.UpdateObjectDeliveryStatus(common.Error, "Error", test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
			dest1.DestType, dest1.DestID); err != nil {
			t.Errorf("UpdateObjectDeliveryStatus failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if dests, err := store.GetObjectDestinationsList(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("GetObjectDestinationsList failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(dests) == 0 {
				t.Errorf("GetObjectDestinationsList returned no destinations (objectID = %s).\n", test.metaData.ObjectID)
			}
			for _, d := range dests {
				if (d.Status != common.Error || d.Message != "Error") && d.Destination != dest2 {
					t.Errorf("GetObjectDestinations returned wrong status or message: (%s, %s) instead of (error, Error) (objectID = %s).\n", d.Status,
						d.Message, test.metaData.ObjectID)
				}
			}
		}
	}
}

func TestMongoStorageWebhooks(t *testing.T) {
	testStorageWebhooks(mongo, t)
}

func TestMongoStorageOrganizations(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	initialNumberOfOrgs := 0
	if orgs, err := store.RetrieveOrganizations(); err != nil {
		t.Errorf("RetrieveOrganizations failed. Error: %s\n", err.Error())
	} else if orgs != nil {
		initialNumberOfOrgs = len(orgs)
	}

	tests := []common.Organization{
		{OrgID: "org1", User: "key1", Password: "secret1", Address: "tcp://abc:1883"},
		{OrgID: "org1", User: "key2", Password: "secret2", Address: "tcp://abc:2883"},
		{OrgID: "org3", User: "key3", Password: "secret3", Address: "tcp://abc:2883"},
	}

	for _, test := range tests {
		if _, err := store.StoreOrganization(test); err != nil {
			t.Errorf("StoreOrganization failed. Error: %s\n", err.Error())
		}
		if org, err := store.RetrieveOrganizationInfo(test.OrgID); err != nil {
			t.Errorf("RetrieveOrganizationInfo failed. Error: %s\n", err.Error())
		} else {
			if org.Org.OrgID != test.OrgID {
				t.Errorf("RetrieveOrganizationInfo returned org with incorrect id: %s instead of %s\n", org.Org.OrgID, test.OrgID)
			}
			if org.Org.User != test.User {
				t.Errorf("RetrieveOrganizationInfo returned org with incorrect User: %s instead of %s\n", org.Org.User, test.User)
			}
			if org.Org.Password != test.Password {
				t.Errorf("RetrieveOrganizationInfo returned org with incorrect Password: %s instead of %s\n", org.Org.Password, test.Password)
			}
			if org.Org.Address != test.Address {
				t.Errorf("RetrieveOrganizationInfo returned org with incorrect Address: %s instead of %s\n", org.Org.Address, test.Address)
			}
		}
	}

	if orgs, err := store.RetrieveOrganizations(); err != nil {
		t.Errorf("RetrieveOrganizations failed. Error: %s\n", err.Error())
	} else if len(orgs)-initialNumberOfOrgs != len(tests)-1 { // there are two tests with the same org id
		t.Errorf("RetrieveOrganizations returned incorrect number of orgs: %d instead of %d\n", len(orgs)-initialNumberOfOrgs, len(tests)-1)
	}

	for _, test := range tests {
		if err := store.DeleteOrganizationInfo(test.OrgID); err != nil {
			t.Errorf("DeleteOrganizationInfo failed. Error: %s\n", err.Error())
		}
		if org, err := store.RetrieveOrganizationInfo(test.OrgID); err != nil {
			t.Errorf("RetrieveOrganizationInfo failed. Error: %s\n", err.Error())
		} else if org != nil {
			t.Errorf("RetrieveOrganizationInfo returned org (%s) for non-existing org\n", org.Org.OrgID)
		}
	}

	if orgs, err := store.RetrieveOrganizations(); err != nil {
		t.Errorf("RetrieveOrganizations failed. Error: %s\n", err.Error())
	} else if len(orgs)-initialNumberOfOrgs != 0 {
		t.Errorf("RetrieveOrganizations returned incorrect number of orgs: %d instead of %d\n", len(orgs)-initialNumberOfOrgs, 0)
	}
}

func TestMongoStorageInactiveDestinations(t *testing.T) {
	store := &MongoStorage{}
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
		return
	}
	defer store.Stop()

	destinations := []struct {
		dest common.Destination
	}{
		{common.Destination{DestOrgID: "inactiveorg1", DestID: "1", DestType: "device", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "inactiveorg2", DestID: "2", DestType: "device", Communication: common.HTTPProtocol}},
	}

	for _, d := range destinations {
		if err := store.StoreDestination(d.dest); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}

		if exists, err := store.DestinationExists(d.dest.DestOrgID, d.dest.DestType, d.dest.DestID); err != nil || !exists {
			t.Errorf("Stored destination doesn't exist\n")
		}
	}

	notifications := []struct {
		n               common.Notification
		shouldBeRemoved bool
	}{
		{common.Notification{ObjectID: "1", ObjectType: "type1", DestOrgID: "inactiveorg1", DestID: "1", DestType: "device",
			Status: common.Update, InstanceID: 5}, true},
		{common.Notification{ObjectID: "2", ObjectType: "type1", DestOrgID: "inactiveorg1", DestID: "2", DestType: "device",
			Status: common.Update, InstanceID: 5}, false},
		{common.Notification{ObjectID: "3", ObjectType: "type1", DestOrgID: "inactiveorg1", DestID: "1", DestType: "device",
			Status: common.Getdata}, true},
		{common.Notification{ObjectID: "4", ObjectType: "type1", DestOrgID: "inactiveorg2", DestID: "2", DestType: "device",
			Status: common.Data}, true},
		{common.Notification{ObjectID: "5", ObjectType: "type1", DestOrgID: "inactiveorg2", DestID: "1", DestType: "device",
			Status: common.AckConsumed, InstanceID: 9}, false},
		{common.Notification{ObjectID: "6", ObjectType: "type1", DestOrgID: "inactiveorg3", DestID: "1", DestType: "device",
			Status: common.AckConsumed, InstanceID: 9}, false},
	}

	for _, n := range notifications {
		if err := store.UpdateNotificationRecord(n.n); err != nil {
			t.Errorf("UpdateNotificationRecord failed. Error: %s\n", err.Error())
		}

		if storedNotification, err := store.RetrieveNotificationRecord(n.n.DestOrgID, n.n.ObjectType, n.n.ObjectID, n.n.DestType,
			n.n.DestID); err != nil {
			t.Errorf("RetrieveNotificationRecord failed. Error: %s\n", err.Error())
		} else if storedNotification == nil {
			t.Errorf("RetrieveNotificationRecord returned nil notification\n")
		}
	}

	store.RemoveInactiveDestinations(time.Now().Add(time.Hour * 24))

	for _, d := range destinations {
		if exists, _ := store.DestinationExists(d.dest.DestOrgID, d.dest.DestType, d.dest.DestID); exists {
			t.Errorf("Inactive destination exists\n")
		}
	}

	for _, n := range notifications {
		if storedNotification, err := store.RetrieveNotificationRecord(n.n.DestOrgID, n.n.ObjectType, n.n.ObjectID, n.n.DestType,
			n.n.DestID); (err == nil || storedNotification != nil) && n.shouldBeRemoved {
			t.Errorf("Notification wasn't removed (object ID = %s)\n", n.n.ObjectID)
		}
	}

}
