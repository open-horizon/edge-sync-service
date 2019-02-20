package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

const (
	boltdb   = "bolt"
	inMemory = "inMemory"
	mongo    = "mongo"
)

func testStorageObjects(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		metaData common.MetaData
		status   string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device",
			Inactive: true}, common.PartiallyReceived},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "device",
			Version: "123", Description: "abc", Inactive: true},
			common.NotReadyToSend},
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
		storedMetaData, storedStatus, err := store.RetrieveObjectAndStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object and status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if storedStatus != test.status {
				t.Errorf("Incorrect object's status (objectID = %s): %s instead of %s\n", test.metaData.ObjectID,
					storedStatus, test.status)
			}
			if storedMetaData.ObjectID != test.metaData.ObjectID {
				t.Errorf("Incorrect object's ID: %s instead of %s\n", storedMetaData.ObjectID, test.metaData.ObjectID)
			}
			if storedMetaData.ObjectType != test.metaData.ObjectType {
				t.Errorf("Incorrect object's type (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.ObjectType, test.metaData.ObjectType)
			}
			if storedMetaData.DestOrgID != test.metaData.DestOrgID {
				t.Errorf("Incorrect object's organization (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.DestOrgID, test.metaData.DestOrgID)
			}
			if storedMetaData.DestID != test.metaData.DestID {
				t.Errorf("Incorrect object's destination ID (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.DestID, test.metaData.DestID)
			}
			if storedMetaData.DestType != test.metaData.DestType {
				t.Errorf("Incorrect object's destination type (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.DestType, test.metaData.DestType)
			}
			if storedMetaData.OriginID != test.metaData.OriginID {
				t.Errorf("Incorrect object's origin ID (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.OriginID, test.metaData.OriginID)
			}
			if storedMetaData.OriginType != test.metaData.OriginType {
				t.Errorf("Incorrect object's origin type (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.OriginType, test.metaData.OriginType)
			}
			if storedMetaData.Expiration != test.metaData.Expiration {
				t.Errorf("Incorrect object's expiration (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.Expiration, test.metaData.Expiration)
			}
			if storedMetaData.Version != test.metaData.Version {
				t.Errorf("Incorrect object's version (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.Version, test.metaData.Version)
			}
			if storedMetaData.Description != test.metaData.Description {
				t.Errorf("Incorrect object's description (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.Description, test.metaData.Description)
			}
			if storedMetaData.Link != test.metaData.Link {
				t.Errorf("Incorrect object's link (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.Link, test.metaData.Link)
			}
			if storedMetaData.ActivationTime != test.metaData.ActivationTime {
				t.Errorf("Incorrect object's activation time (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.ActivationTime, test.metaData.ActivationTime)
			}
			if storedMetaData.DestinationDataURI != test.metaData.DestinationDataURI {
				t.Errorf("Incorrect object's data URI (objectID = %s): %s instead of %s\n", storedMetaData.ObjectID,
					storedMetaData.DestinationDataURI, test.metaData.DestinationDataURI)
			}
			if storedMetaData.Inactive != test.metaData.Inactive {
				t.Errorf("Incorrect object's Inactive flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.Inactive, test.metaData.Inactive)
			}
			if storedMetaData.Inactive != test.metaData.Inactive {
				t.Errorf("Incorrect object's Inactive flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.Inactive, test.metaData.Inactive)
			}
			if storedMetaData.NoData != test.metaData.NoData {
				t.Errorf("Incorrect object's NoData flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.NoData, test.metaData.NoData)
			}
			if storedMetaData.MetaOnly != test.metaData.MetaOnly {
				t.Errorf("Incorrect object's MetaOnly flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.MetaOnly, test.metaData.MetaOnly)
			}
			if storedMetaData.Deleted != test.metaData.Deleted {
				t.Errorf("Incorrect object's Deleted flag (objectID = %s): %t instead of %t\n", storedMetaData.ObjectID,
					storedMetaData.Deleted, test.metaData.Deleted)
			}
			if storedMetaData.ExpectedConsumers != test.metaData.ExpectedConsumers {
				t.Errorf("Incorrect object's expected consumers (objectID = %s): %d instead of %d\n", storedMetaData.ObjectID,
					storedMetaData.ExpectedConsumers, test.metaData.ExpectedConsumers)
			}
		}

		instanceID := storedMetaData.InstanceID
		// Update, instance ID for the sending side should be incremented
		if err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedMetaData, storedStatus, err = store.RetrieveObjectAndStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object and status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if test.status == common.NotReadyToSend && storedMetaData.InstanceID <= instanceID {
			t.Errorf("Incorrect object's instance ID (objectID = %s): %d should be greater than %d\n", storedMetaData.ObjectID,
				storedMetaData.InstanceID, instanceID)
		}

		// Consumers
		remainingConsumers, err := store.RetrieveObjectRemainingConsumers(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve remainingConsumers (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if remainingConsumers != test.metaData.ExpectedConsumers {
			t.Errorf("Incorrect object's remainig consumers (objectID = %s): %d instead of %d\n", storedMetaData.ObjectID,
				remainingConsumers, test.metaData.ExpectedConsumers)
		}

		remainingConsumers, err = store.DecrementAndReturnRemainingConsumers(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to decrement and retrieve remainingConsumers (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if remainingConsumers != test.metaData.ExpectedConsumers-1 {
			t.Errorf("Incorrect object's remainig consumers (objectID = %s): %d instead of %d\n", storedMetaData.ObjectID,
				remainingConsumers, test.metaData.ExpectedConsumers-1)
		}

		if err = store.ResetObjectRemainingConsumers(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to reset remainingConsumers (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		remainingConsumers, err = store.RetrieveObjectRemainingConsumers(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve remainingConsumers (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if remainingConsumers != test.metaData.ExpectedConsumers {
			t.Errorf("Incorrect object's remainig consumers (objectID = %s): %d instead of %d\n", storedMetaData.ObjectID,
				remainingConsumers, test.metaData.ExpectedConsumers)
		}

		// Deleted
		if err := store.MarkObjectDeleted(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to mark object as deleted (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedStatus, err = store.RetrieveObjectStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object's status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if storedStatus != common.ObjDeleted {
				t.Errorf("Object is not marked as deleted (objectID = %s)\n", storedMetaData.ObjectID)
			}
		}

		// Activate
		if err := store.ActivateObject(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to activate object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedMetaData, err = store.RetrieveObject(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if storedMetaData.Inactive {
				t.Errorf("Object is not marked as active (objectID = %s)\n", storedMetaData.ObjectID)
			}
		}

		// Status
		if err := store.UpdateObjectStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID, "status"); err != nil {
			t.Errorf("Failed to update status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		if status, err := store.RetrieveObjectStatus(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to retrieve status (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if status != "status" {
			t.Errorf("Incorrect status (objectID = %s): %s instead of status\n", test.metaData.ObjectID, status)
		}

	}

	// There are no updated objects
	objects, err := store.RetrieveUpdatedObjects(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType, false)
	if err != nil {
		t.Errorf("RetrieveUpdatedObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 0 {
		t.Errorf("RetrieveUpdatedObjects returned objects\n")
	}

	// There are no objects to send
	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 0 {
		t.Errorf("RetrieveObjects returned objects\n")
	}
}

func testStorageObjectActivation(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	common.Configuration.StorageMaintenanceInterval = 1

	activationTime1 := time.Now().Add(time.Second * 1).Format(time.RFC3339)
	activationTime2 := time.Now().Add(time.Second * 2).Format(time.RFC3339)

	tests := []struct {
		metaData common.MetaData
		status   string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device"},
			common.ReadyToSend},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device",
			Inactive: true, ActivationTime: activationTime1}, common.ReadyToSend},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device",
			Inactive: true, ActivationTime: activationTime2}, common.ReadyToSend},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device",
			Inactive: true, ActivationTime: activationTime1}, common.PartiallyReceived},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device",
			Inactive: true, ActivationTime: activationTime2}, common.PartiallyReceived},
	}

	for _, test := range tests {
		// Insert
		if err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	objectsToActivate, statuses, err := store.GetObjectsToActivate()
	if err != nil {
		t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
	}
	if len(objectsToActivate) != 0 || len(statuses) != 0 {
		t.Errorf("GetObjectsToActivate returned objects.\n")
	}
	select {
	case <-time.After(1 * time.Second):
		objectsToActivate, statuses, err := store.GetObjectsToActivate()
		if err != nil {
			t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
		} else {
			if len(objectsToActivate) != 1 || len(statuses) != 1 {
				t.Errorf("GetObjectsToActivate returned incorrect number of objects: %d instead of 1.\n", len(objectsToActivate))
			} else if objectsToActivate[0].ObjectID != "2" {
				t.Errorf("GetObjectsToActivate returned incorrect objects: id=%s instead of 2.\n", objectsToActivate[0].ObjectID)
			}
		}
	}
	select {
	case <-time.After(2 * time.Second):
		objectsToActivate, statuses, err := store.GetObjectsToActivate()
		if err != nil {
			t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
		} else {
			if len(objectsToActivate) != 2 || len(statuses) != 2 {
				t.Errorf("GetObjectsToActivate returned incorrect number of objects: %d instead of 2.\n", len(objectsToActivate))
			} else if (objectsToActivate[0].ObjectID != "2" && objectsToActivate[0].ObjectID != "3") ||
				(objectsToActivate[1].ObjectID != "2" && objectsToActivate[1].ObjectID != "3") {
				t.Errorf("GetObjectsToActivate returned incorrect objects.\n")
			}
		}
	}
}

func testStorageObjectData(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		metaData common.MetaData
		status   string
		data     []byte
		newData  []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device"},
			common.ReadyToSend, []byte("abcdefghijklmnopqrstuvwxyz"), []byte("new")},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device",
			Inactive: true}, common.CompletelyReceived, []byte("abcdefghijklmnopqrstuvwxyz"), []byte("new")},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg", DestID: "dev1", DestType: "device"},
			common.ReadyToSend, nil, []byte("new")},
	}

	for _, test := range tests {
		test.metaData.ObjectSize = int64(len(test.data) + len(test.newData))
		// Insert
		if err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}

		// Check stored data
		dataReader, err := store.RetrieveObjectData(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object's data' (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if dataReader == nil {
			if test.data != nil {
				t.Errorf("No data for objectID = %s", test.metaData.ObjectID)
			}
		} else {
			data := make([]byte, 100)
			n, err := dataReader.Read(data)
			if err != nil {
				t.Errorf("Failed to read object's data from the returned reader (objectID = %s). Error: %s",
					test.metaData.ObjectID, err.Error())
			}
			if n != len(test.data) {
				t.Errorf("Incorrect data size 's data from the returned reader (objectID = %s): %d instead of %d",
					test.metaData.ObjectID, n, len(test.data))
			}
			data = data[:n]
			if string(data) != string(test.data) {
				t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
					test.metaData.ObjectID, string(data), string(test.data))
			}
			store.CloseDataReader(dataReader)
		}

		// Read data with offset
		if test.data != nil {
			data, eof, _, err := store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				26, 0)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if string(data) != string(test.data) {
					t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
						test.metaData.ObjectID, string(data), string(test.data[2:6]))
				}
				if !eof {
					t.Errorf("ReadObjectData returned eof=false (objectID = %s)", test.metaData.ObjectID)
				}
			}

			data, eof, read, err := store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				6, 26)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if read != 0 {
					t.Errorf("Incorrect data (objectID = %s): %s instead of empty data",
						test.metaData.ObjectID, string(data))
				}
				if !eof {
					t.Errorf("ReadObjectData returned eof=false (objectID = %s)", test.metaData.ObjectID)
				}
			}

			data, eof, _, err = store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				4, 2)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if string(data) != string(test.data[2:6]) {
					t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
						test.metaData.ObjectID, string(data), string(test.data[2:6]))
				}
				if eof {
					t.Errorf("ReadObjectData returned eof=true (objectID = %s)", test.metaData.ObjectID)
				}
			}

			// Offset > data size
			data, _, read, err = store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				4, 200)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if read != 0 {
					t.Errorf("Incorrect data (objectID = %s): should be empty", test.metaData.ObjectID)
				}
			}

			// Size > data size
			data, _, read, err = store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				400, 2)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if read != len(test.data)-2 {
					t.Errorf("Incorrect data size (objectID = %s): %d instead of %d", test.metaData.ObjectID, len(data), len(test.data)-2)
				}
			}
		}

		// Store new data
		if ok, err := store.StoreObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
			bytes.NewReader(test.newData)); err != nil {
			t.Errorf("StoreObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else if !ok {
			t.Errorf("StoreObjectData failed to find object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			data, _, _, err := store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				len(test.newData), 0)
			if err != nil {
				t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if len(data) != len(test.newData) {
					t.Errorf("Incorrect data size (objectID = %s): %d instead of %d", test.metaData.ObjectID, len(data), len(test.newData))
				}
				if string(data) != string(test.newData) {
					t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
						test.metaData.ObjectID, string(data), string(test.newData))
				}
			}
		}

		// Append data
		if test.data != nil {
			if err := store.AppendObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				bytes.NewReader(test.data), uint32(len(test.data)), 0, test.metaData.ObjectSize, true, false); err != nil {
				t.Errorf("AppendObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else if err := store.AppendObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				bytes.NewReader(test.newData), uint32(len(test.newData)), int64(len(test.data)), test.metaData.ObjectSize, false, true); err != nil {
				t.Errorf("AppendObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				expectedData := append(test.data, test.newData...)
				data, _, _, err := store.ReadObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
					len(expectedData), 0)
				if err != nil {
					t.Errorf("ReadObjectData failed (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				} else {
					if len(data) != len(expectedData) {
						t.Errorf("Incorrect data size (objectID = %s): %d instead of %d", test.metaData.ObjectID,
							len(data), len(expectedData))
					}
					if string(data) != string(expectedData) {
						t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
							test.metaData.ObjectID, string(data), string(expectedData))
					}
				}
			}
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

	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 2 {
		t.Errorf("RetrieveObjects returned wrong number of objects: %d instead of 2\n", len(objects))
	}
}

func testStorageNotifications(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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
		{common.Notification{ObjectID: "6", ObjectType: "type1", DestOrgID: "myorg123", DestID: "1", DestType: "device2",
			Status: common.DeletePending}},
	}

	for _, test := range tests {
		if err := store.UpdateNotificationRecord(test.n); err != nil {
			t.Errorf("UpdateNotificationRecord failed. Error: %s\n", err.Error())
		}
		if n, err := store.RetrieveNotificationRecord(test.n.DestOrgID, test.n.ObjectType, test.n.ObjectID, test.n.DestType,
			test.n.DestID); err != nil {
			t.Errorf("RetrieveNotificationRecord failed. Error: %s\n", err.Error())
		} else {
			if test.n.DestOrgID != n.DestOrgID {
				t.Errorf("Retrieved notification DestOrgID (%s) is different from the stored one(%s)\n", n.DestOrgID, test.n.DestOrgID)
			}
			if test.n.DestType != n.DestType {
				t.Errorf("Retrieved notification DestType (%s) is different from the stored one(%s)\n", n.DestType, test.n.DestType)
			}
			if test.n.DestID != n.DestID {
				t.Errorf("Retrieved notification DestID (%s) is different from the stored one(%s)\n", n.DestID, test.n.DestID)
			}
			if test.n.ObjectType != n.ObjectType {
				t.Errorf("Retrieved notification ObjectType (%s) is different from the stored one(%s)\n", n.ObjectType, test.n.ObjectType)
			}
			if test.n.ObjectID != n.ObjectID {
				t.Errorf("Retrieved notification ObjectID (%s) is different from the stored one(%s)\n", n.ObjectID, test.n.ObjectID)
			}
			if test.n.Status != n.Status {
				t.Errorf("Retrieved notification Status (%s) is different from the stored one(%s)\n", n.Status, test.n.Status)
			}
			if test.n.InstanceID != n.InstanceID {
				t.Errorf("Retrieved notification InstanceID (%d) is different from the stored one(%d)\n", n.InstanceID, test.n.InstanceID)
			}
			if resend := time.Unix(n.ResendTime, 0); resend.Unix() != n.ResendTime {
				t.Errorf("Retrieved notification has invalid ResendTime %d. \n", n.ResendTime)
			} else if time.Now().After(resend) {
				t.Errorf("Retrieved notification has ResendTime in the past\n")
			}
		}
	}

	if notifications, err := store.RetrieveNotifications(tests[0].n.DestOrgID, tests[0].n.DestType, tests[0].n.DestID, false); err != nil {
		t.Errorf("RetrieveNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 2 {
		t.Errorf("RetrieveNotifications returned wrong number of notifications: %d instead of 2\n", len(notifications))
	}

	if notifications, err := store.RetrievePendingNotifications(tests[5].n.DestOrgID, tests[5].n.DestType,
		tests[5].n.DestID); err != nil {
		t.Errorf("RetrievePendingNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 1 && storageType == mongo {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 1\n", len(notifications))
	} else if len(notifications) != 0 && storageType == inMemory {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 0\n", len(notifications))
	}

	if notifications, err := store.RetrievePendingNotifications(tests[5].n.DestOrgID, "", ""); err != nil {
		t.Errorf("RetrievePendingNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 2 && storageType == mongo {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 2\n", len(notifications))
	} else if len(notifications) != 0 && storageType == inMemory {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 0\n", len(notifications))
	}

	if err := store.DeleteNotificationRecords(tests[0].n.DestOrgID, tests[0].n.ObjectType, tests[0].n.ObjectID, "", ""); err != nil {
		t.Errorf("DeleteNotificationRecords failed. Error: %s\n", err.Error())
	} else {
		if n, err := store.RetrieveNotificationRecord(tests[0].n.DestOrgID, tests[0].n.ObjectType, tests[0].n.ObjectID,
			tests[0].n.DestType, tests[0].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
		if n, err := store.RetrieveNotificationRecord(tests[1].n.DestOrgID, tests[1].n.ObjectType, tests[1].n.ObjectID,
			tests[1].n.DestType, tests[1].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
	}

	if err := store.DeleteNotificationRecords(tests[2].n.DestOrgID, tests[2].n.ObjectType, tests[2].n.ObjectID,
		tests[2].n.DestType, tests[2].n.DestID); err != nil {
		t.Errorf("DeleteNotificationRecords failed. Error: %s\n", err.Error())
	} else {
		if n, err := store.RetrieveNotificationRecord(tests[2].n.DestOrgID, tests[2].n.ObjectType, tests[2].n.ObjectID,
			tests[2].n.DestType, tests[2].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
	}

	if err := store.DeleteNotificationRecords(tests[3].n.DestOrgID, "", "",
		tests[3].n.DestType, tests[3].n.DestID); err != nil {
		t.Errorf("DeleteNotificationRecords failed. Error: %s\n", err.Error())
	} else {
		if n, err := store.RetrieveNotificationRecord(tests[3].n.DestOrgID, tests[3].n.ObjectType, tests[3].n.ObjectID,
			tests[3].n.DestType, tests[3].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
		if n, err := store.RetrieveNotificationRecord(tests[4].n.DestOrgID, tests[4].n.ObjectType, tests[4].n.ObjectID,
			tests[4].n.DestType, tests[4].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
		if n, err := store.RetrieveNotificationRecord(tests[5].n.DestOrgID, tests[5].n.ObjectType, tests[5].n.ObjectID,
			tests[5].n.DestType, tests[5].n.DestID); err == nil && n != nil {
			t.Errorf("RetrieveNotificationRecord returned notification after its deletion.\n")
		}
	}
}

func testStorageWebhooks(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		orgID      string
		objectType string
		url        string
	}{
		{"111", "t1", "abc/xyz"},
		{"111", "t1", "http://abc/xyz/111"},
		{"111", "t1", "http://abc/xyz"},
		{"111", "t2", "http://abc/xyz"},
		{"111", "t2", "http://abc/xyz/222"},
	}

	// Add all the webhooks
	for _, test := range tests {
		if err := store.AddWebhook(test.orgID, test.objectType, test.url); err != nil {
			t.Errorf("Failed to add webhook. Error: %s\n", err.Error())
		}
	}

	// Retrieve t1 webhooks
	if hooks, err := store.RetrieveWebhooks(tests[0].orgID, tests[0].objectType); err != nil {
		t.Errorf("Failed to retrieve webhooks. Error: %s\n", err.Error())
	} else {
		if hooks == nil {
			t.Errorf("RetrieveWebhooks returned nil\n")
		} else {
			if len(hooks) != 3 {
				t.Errorf("RetrieveWebhooks returned %d webhooks instead of 3\n", len(hooks))
			}
		}
	}

	// Delete one of the t1 hooks
	if err := store.DeleteWebhook(tests[1].orgID, tests[1].objectType, tests[1].url); err != nil {
		t.Errorf("Failed to delete webhook. Error: %s\n", err.Error())
	}
	if hooks, err := store.RetrieveWebhooks(tests[0].orgID, tests[0].objectType); err != nil {
		t.Errorf("Failed to retrieve webhooks. Error: %s\n", err.Error())
	} else {
		if hooks == nil {
			t.Errorf("RetrieveWebhooks returned nil\n")
		} else {
			if len(hooks) != 2 {
				t.Errorf("RetrieveWebhooks returned %d webhooks instead of 2\n", len(hooks))
			} else {
				if hooks[0] != tests[0].url || hooks[1] != tests[2].url {
					t.Errorf("RetrieveWebhooks returned incorrect webhooks \n")
				}
			}
		}
	}

	// Delete the remaining two t1 hooks
	if err := store.DeleteWebhook(tests[0].orgID, tests[0].objectType, tests[0].url); err != nil {
		t.Errorf("Failed to delete webhook. Error: %s\n", err.Error())
	}
	if err := store.DeleteWebhook(tests[2].orgID, tests[2].objectType, tests[2].url); err != nil {
		t.Errorf("Failed to delete webhook. Error: %s\n", err.Error())
	}
	if hooks, err := store.RetrieveWebhooks(tests[0].orgID, tests[0].objectType); err == nil || hooks != nil {
		t.Errorf("RetrieveWebhooks returned webhhoks after all the hooks were deleted\n")
	}
}

func setUpStorage(storageType string) (Storage, error) {
	var store Storage
	if storageType == inMemory {
		store = &Cache{Store: &InMemoryStorage{}}
	} else if storageType == boltdb {
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStor := &BoltStorage{}
		boltStor.Cleanup()
		store = &Cache{Store: boltStor}
	} else {
		store = &Cache{Store: &MongoStorage{}}
	}
	if err := store.Init(); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())}
	}
	return store, nil
}
