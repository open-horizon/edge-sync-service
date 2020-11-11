package storage

import (
	"github.com/open-horizon/edge-sync-service/common"
	"testing"
	"time"
)

func TestCouchStorageConnection(t *testing.T) {
	store, err := setUpStorage(common.Couch)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()
}

func TestStorageObjects(t *testing.T) {
	store, err := setUpStorage(common.Couch)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		metaData common.MetaData
		status   string
		data     []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend, []byte("Attachment 1")},

		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device", MetaOnly: true,
			Inactive: true}, common.PartiallyReceived, nil},

		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device",
			Version: "123", Description: "abc", Inactive: true}, common.NotReadyToSend, nil},

		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device", NoData: true, MetaOnly: true},
			common.NotReadyToSend, nil},

		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device", NoData: true},
			common.NotReadyToSend, nil},

		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend, []byte("Attachment 5")},

		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device", NoData: true},
			common.NotReadyToSend, nil},
	}

	for _, test := range tests {
		// Insert
		if deletedDests, err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(deletedDests) != 0 {
				t.Errorf("StoreObject for new object returned deleted destinations (objectID = %s)\n", test.metaData.ObjectID)
			}
		}

		storedMetaData, _, err := store.RetrieveObjectAndStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object and status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if storedMetaData.NoData != test.metaData.NoData {
				t.Errorf("Incorrect object's NoData flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.NoData, test.metaData.NoData)
			}
			if storedMetaData.MetaOnly != test.metaData.MetaOnly {
				t.Errorf("Incorrect object's MetaOnly flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.MetaOnly, test.metaData.MetaOnly)
			}

		}

		instanceID := storedMetaData.InstanceID
		// Update, instance ID for the sending side should be incremented
		time.Sleep(20 * time.Millisecond)
		if _, err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedMetaData, _, err = store.RetrieveObjectAndStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object and status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if test.status == common.NotReadyToSend && storedMetaData.InstanceID <= instanceID {
			t.Errorf("Incorrect object's instance ID (objectID = %s): %d should be greater than %d\n", storedMetaData.ObjectID,
				storedMetaData.InstanceID, instanceID)
		}

	}
}
