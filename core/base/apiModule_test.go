package base

import (
	"bytes"
	"math"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func TestObjectAPI(t *testing.T) {
	dests := []string{"device:dev1", "device2:dev", "device2:dev1"}

	invalidObjects := []struct {
		orgID      string
		objectType string
		objectID   string
		metaData   common.MetaData
		data       []byte
		message    string
	}{
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777"}, nil,
			"updateObject didn't compare objectID to the objectID of the meta data"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "", ObjectType: "type1", DestOrgID: "myorg777"}, nil,
			"updateObject didn't check meta data objectID to be not empty"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type2", DestOrgID: "myorg777"}, nil,
			"updateObject didn't compare objectType to the objectType of the meta data"},
		{"myorg777", "", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777"}, nil,
			"updateObject didn't check meta data objectType to be not empty"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", Expiration: "2029:01:02T15:04:05+07:00"}, nil,
			"updateObject didn't check expiration format"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", Expiration: "2009-01-02T15:04:05+07:00"}, nil,
			"updateObject didn't check if the expiration time has past"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", DestID: "dev1"}, nil,
			"updateObject didn't check that destType is not empty if destID is not empty"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
			Inactive: true, ActivationTime: "2029:01:02T15:04:05+07:00"}, nil,
			"updateObject didn't check activation time format"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
			Inactive: true, ActivationTime: "2009-01-02T15:04:05+07:00"}, nil,
			"updateObject didn't check if the activation time has past"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", Deleted: true}, nil,
			"updateObject didn't check that the object is marked as deleted"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", DestinationDataURI: "file://host/xyz"}, nil,
			"updateObject didn't check that DestinationDataURI has a host"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", DestinationDataURI: "http://xyz"}, nil,
			"updateObject didn't check the DestinationDataURI's scheme"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", SourceDataURI: "file://host/xyz"}, nil,
			"updateObject didn't check that SourceDataURI has a host"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", SourceDataURI: "http://xyz"}, nil,
			"updateObject didn't check the SourceDataURI's scheme"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", SourceDataURI: "file:///xyz"}, []byte("data"),
			"updateObject didn't check that both SourceDataURI and data are set"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
			DestID: "dev1", DestType: "dev1", DestinationsList: dests}, nil,
			"updateObject didn't check that destType, destId and destinationsList are not empty"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
			DestType: "dev1", AutoDelete: true}, nil,
			"updateObject didn't check that autoDelete is set for object without destinations list or dest ID"},
	}

	communications.Store = &storage.MongoStorage{}
	store = communications.Store
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	for _, row := range invalidObjects {
		err := updateObject(row.orgID, row.objectType, row.objectID, row.metaData, nil)
		if err == nil {
			t.Errorf(row.message)
		}
	}

	validObjects := []struct {
		orgID              string
		objectType         string
		objectID           string
		metaData           common.MetaData
		data               []byte
		expectedStatus     string
		expectedConsumers  int
		newData            []byte
		expectedDestNumber int
	}{
		{"myorg777", "type1", "1", common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg777",
			DestID: "dev1", DestType: "device"},
			nil, common.NotReadyToSend, 1, []byte("new"), 1},
		{"myorg777", "type1", "2", common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: -1, NoData: true, Link: "abc", DestID: "dev1", DestType: "device"},
			[]byte("abc"), common.ReadyToSend, math.MaxInt32, []byte("new"), 1},
		{"myorg777", "type1", "3", common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, Link: "abc", DestID: "dev2", DestType: "device"},
			nil, common.ReadyToSend, 3, []byte("new"), 0},
		{"myorg777", "type1", "3", common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, MetaOnly: true, DestID: "dev2", DestType: "device"},
			nil, common.ReadyToSend, 3, []byte("new"), 0},
		{"myorg777", "type1", "4", common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1"},
			[]byte("abc"), common.ReadyToSend, 3, []byte("new"), 1},
		{"myorg777", "type1", "5", common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", Inactive: true},
			[]byte("abc"), common.ReadyToSend, 3, []byte("new"), 1},
		{"myorg777", "type1", "6", common.MetaData{ObjectID: "6", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 1, DestinationsList: dests},
			[]byte("abc"), common.ReadyToSend, 1, []byte("new"), 3},
	}

	destination := common.Destination{DestOrgID: "myorg777", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	if err := store.StoreDestination(common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev",
		Communication: common.MQTTProtocol}); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}
	if err := store.StoreDestination(common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev1",
		Communication: common.MQTTProtocol}); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	communications.Comm = &communications.TestComm{}
	if err := communications.Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	for _, row := range validObjects {
		// Update object
		err := updateObject(row.orgID, row.objectType, row.objectID, row.metaData, row.data)
		if err != nil {
			t.Errorf("updateObject failed to update (objectID = %s). Error: %s", row.objectID, err.Error())
		}

		// Retrieve object's meta data and status, and check them
		metaData, status, err := store.RetrieveObjectAndStatus(row.orgID, row.objectType, row.objectID)
		if err != nil {
			t.Errorf("Failed to fetch updated object (objectID = %s).", row.objectID)
		}
		if status != row.expectedStatus {
			t.Errorf("Wrong status: %s instead of %s (objectID = %s)", status, row.expectedStatus, row.objectID)
		}
		if metaData.DestOrgID != row.orgID {
			t.Errorf("Wrong DestOrgID: %s instead of %s (objectID = %s)", metaData.DestOrgID, row.orgID, row.objectID)
		}
		if metaData.ObjectType != row.objectType {
			t.Errorf("Wrong objectType: %s instead of %s (objectID = %s)", metaData.ObjectType, row.objectType, row.objectID)
		}
		if metaData.ObjectID != row.objectID {
			t.Errorf("Wrong DestOrgID: %s instead of %s (objectID = %s)", metaData.ObjectID, row.objectID, row.objectID)
		}
		if metaData.ExpectedConsumers != row.expectedConsumers {
			t.Errorf("Wrong number of expected consumers: %d instead of %d (objectID = %s)", metaData.ExpectedConsumers, row.expectedConsumers, row.objectID)
		}

		// Get data
		dataReader, err := store.RetrieveObjectData(row.orgID, row.objectType, row.objectID)
		if err != nil {
			t.Errorf("An error occurred in data fetch (objectID = %s). Error: %s", row.objectID, err.Error())
		}
		if row.metaData.NoData && (metaData.Link != "" || dataReader != nil) {
			t.Errorf("Data in object with NoData set to true (objectID = %s)", row.objectID)
		}
		if !row.metaData.NoData && row.data != nil && dataReader == nil {
			t.Errorf("Failed to fetch object's data (objectID = %s)", row.objectID)
		}

		// Check the created notification
		if row.expectedStatus == common.ReadyToSend && !metaData.Inactive {
			if destination.DestID == metaData.DestID {
				notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
				if err != nil {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
				}
				if (metaData.DestType == destination.DestType || metaData.DestType == "") &&
					(metaData.DestID == destination.DestID || metaData.DestID == "") {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.objectID)
					} else {
						if notification.Status != common.Update {
							t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status, row.objectID)
						}
						if notification.InstanceID != metaData.InstanceID {
							t.Errorf("Wrong instance ID in notification: %d instead of %d (objectID = %s)", notification.InstanceID,
								metaData.InstanceID, row.objectID)
						}
					}
				}
			} else if metaData.DestinationsList != nil {
				if destinations, err := store.GetObjectDestinations(*metaData); err != nil {
					t.Errorf("GetObjectDestinations failed. Error: %s", err.Error())
				} else {
					for _, d := range destinations {
						notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, d.DestType, d.DestID)
						if err != nil {
							t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
						}
						if notification == nil {
							t.Errorf("No notification record (objectID = %s)", row.objectID)
						} else {
							if notification.Status != common.Update {
								t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status, row.objectID)
							}
							if notification.InstanceID != metaData.InstanceID {
								t.Errorf("Wrong instance ID in notification: %d instead of %d (objectID = %s)", notification.InstanceID,
									metaData.InstanceID, row.objectID)
							}
						}
					}
				}
			}
		}

		// Check other object APIs
		// Get status
		storedStatus, err := getObjectStatus(row.orgID, row.objectType, row.objectID)
		if err != nil {
			t.Errorf("Failed to get object's status (objectID = %s). Error: %s", row.objectID, err.Error())
		}
		if storedStatus != row.expectedStatus {
			t.Errorf("getObjectStatus returned incorrect status: %s instead of %s", storedStatus, row.expectedStatus)
		}

		// Get data
		if !metaData.MetaOnly {
			storedDataReader, err := getObjectData(row.orgID, row.objectType, row.objectID)
			if err != nil {
				if storage.IsNotFound(err) {
					if row.data != nil && !row.metaData.NoData {
						t.Errorf("getObjectData failed to get object's data (objectID = %s): data not found", row.objectID)
					}
				} else {
					t.Errorf("getObjectData to get object's data (objectID = %s). Error: %s", row.objectID, err.Error())
				}
			} else {
				if storedDataReader == nil {
					if row.data != nil && !row.metaData.NoData {
						t.Errorf("getObjectData failed to get object's data (objectID = %s): data not found", row.objectID)
					}
				} else {
					storedData := make([]byte, 100)
					n, err := storedDataReader.Read(storedData)
					if err != nil {
						t.Errorf("Failed to read object's data from the returned reader (objectID = %s). Error: %s", row.objectID, err.Error())
					}
					if n != len(row.data) {
						t.Errorf("getObjectData read incorrect data size 's data from the returned reader (objectID = %s): %d instead of %d", row.objectID, n, len(row.data))
					}
					storedData = storedData[:n]
					if string(storedData) != string(row.data) {
						t.Errorf("getObjectData returned incorrect data (objectID = %s): %s instead of %s", row.objectID, string(storedData), string(row.data))
					}
				}
			}
		}

		// Put data
		instance := metaData.InstanceID

		if row.newData != nil {
			ok, err := putObjectData(row.orgID, row.objectType, row.objectID, bytes.NewReader(row.newData))
			if err != nil {
				if !row.metaData.NoData {
					t.Errorf("Failed to update object's data (objectID = %s). Error: %s", row.objectID, err.Error())
				}
			} else {
				if row.metaData.NoData {
					t.Errorf("putObjectData  updated object's data even though NoData flag is set (objectID = %s)", row.objectID)
				} else {
					if !ok {
						t.Errorf("Failed to update object's data (objectID = %s): object not found", row.objectID)
					} else {
						// Data was updated
						storedStatus, err := getObjectStatus(row.orgID, row.objectType, row.objectID)
						if err != nil {
							t.Errorf("Failed to get object's status (objectID = %s). Error: %s", row.objectID, err.Error())
						}
						if storedStatus != common.ReadyToSend {
							t.Errorf("Incorrect status after data update: %s instead of ReadyToSend", storedStatus)
						}

						metaData, err := store.RetrieveObject(row.orgID, row.objectType, row.objectID)
						if err != nil {
							t.Errorf("Failed to fetch updated object after data update (objectID = %s).", row.objectID)
						}
						if row.expectedStatus == common.ReadyToSend && metaData.InstanceID <= instance {
							t.Errorf("Instance ID was not updated: %d should be greater than %d  (objectID = %s)",
								metaData.InstanceID, instance, row.objectID)
						}

						if destination.DestID == metaData.DestID {
							notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
							if err != nil {
								t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
							}

							if !metaData.Inactive && (metaData.DestType == destination.DestType || metaData.DestType == "") &&
								(metaData.DestID == destination.DestID || metaData.DestID == "") {
								if notification == nil {
									t.Errorf("No notification record after data update (objectID = %s)", row.objectID)
								} else {
									if notification.Status != common.Update {
										t.Errorf("Wrong notification status after data update: %s instead of update (objectID = %s)", notification.Status,
											row.objectID)
									}
									if row.expectedStatus == common.ReadyToSend && notification.InstanceID <= instance {
										t.Errorf("Wrong instance ID in notification after data update: %d should be greater than %d (objectID = %s)",
											notification.InstanceID, instance, row.objectID)
									}
								}
							}
						}
					}
				}
			}
		}

		// Mark consumed (should fail)
		if err := objectConsumed(row.orgID, row.objectType, row.objectID); err == nil {
			t.Errorf("objectConsumed marked the sender's object as consumed  (objectID = %s)", row.objectID)
		}

		// Mark deleted (should fail)
		if err := objectDeleted(row.orgID, row.objectType, row.objectID); err == nil {
			t.Errorf("objectDeleted marked the sender's object as deleted  (objectID = %s)", row.objectID)
		}

		// Activate
		if err := activateObject(row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("Failed to activate object (objectID = %s). Error: %s", row.objectID, err.Error())
		} else {
			if destination.DestID == metaData.DestID {
				notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
				}

				if (metaData.DestType == destination.DestType || metaData.DestType == "") &&
					(metaData.DestID == destination.DestID || metaData.DestID == "") {
					if notification == nil {
						t.Errorf("No notification record after object activation (objectID = %s)", row.objectID)
					} else {
						if notification.Status != common.Update {
							t.Errorf("Wrong notification status after object activation: %s instead of update (objectID = %s)", row.objectID,
								notification.Status)
						}
					}
				}
			}
		}

		// Destinations list for the object
		if dests, err := getObjectDestinationsStatus(row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("Error in getObjectDestinationsStatus (objectID = %s). Error: %s", row.objectID, err.Error())
		} else if len(dests) != row.expectedDestNumber {
			t.Errorf("Wrong number of destinations: %d instead of %d (objectID = %s).", len(dests), row.expectedDestNumber, row.objectID)
		}

		// Delete
		if err := deleteObject(row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s", row.objectID, err.Error())
		} else {
			if storedStatus, err := getObjectStatus(row.orgID, row.objectType, row.objectID); err != nil {
				t.Errorf("Error in getObjectStatus (objectID = %s). Error: %s", row.objectID, err.Error())
			} else if storedStatus != common.ObjDeleted {
				t.Errorf("Deleted object is not marked as deleted (objectID = %s)", row.objectID)
			}
		}
	}

	if err := deleteOrganization("myorg777"); err != nil {
		t.Errorf("deleteOrganization failed. Error: %s", err.Error())
	}
}
