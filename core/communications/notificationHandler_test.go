package communications

import (
	"fmt"
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func TestNotificationHandler(t *testing.T) {

	common.InitObjectLocks()
	common.InitObjectDownloadSemaphore()

	if common.Registered {
		t.Errorf("Registered flag is true")
	}
	common.Configuration.NodeType = common.ESS
	common.Configuration.DestinationType = "device"
	common.Configuration.DestinationID = "dev1"
	dest := common.Destination{DestOrgID: "someorg", DestType: common.Configuration.DestinationType, DestID: common.Configuration.DestinationID,
		Communication: common.MQTTProtocol}
	if err := handleRegistration(dest, false); err == nil {
		t.Errorf("ESS registered destination")
	}

	//Store = &storage.InMemoryStorage{}
	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
	common.Configuration.StorageProvider = common.Bolt
	boltStore := &storage.BoltStorage{}
	boltStore.Cleanup(true)
	Store = boltStore
	if err := Store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start communication. Error: %s", err.Error())
	}

	common.Configuration.CommunicationProtocol = common.MQTTProtocol

	common.Configuration.NodeType = common.CSS

	DestReqQueue = NewDestinationRequestQueue(40)
	defer DestReqQueue.Close()

	if err := Store.StoreDestination(dest); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	if err := handleRegistration(dest, false); err != nil {
		t.Errorf("Failed to register destination. Error: %s", err.Error())
	}

	if err := handleRegistration(dest, false); err != nil {
		t.Errorf("Failed to re-register destination. Error: %s", err.Error())
	}

	data1 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, byte(common.Version.Major),
		0, 0, 0, byte(common.Version.Minor),
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '1',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 12,
		0, 0, 0, dataField, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o',
	}
	data2 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, byte(common.Version.Major),
		0, 0, 0, byte(common.Version.Minor),
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '4',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 16,
		0, 0, 0, dataField, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o',
	}
	data3 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, byte(common.Version.Major),
		0, 0, 0, byte(common.Version.Minor),
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '4',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, dataField, 0, 0, 0, 7, ' ', 'w', 'o', 'r', 'l', 'd', '!',
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 16,
	}
	data4 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, byte(common.Version.Major),
		0, 0, 0, byte(common.Version.Minor),
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '5',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, dataField, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 17,
	}

	tests := []struct {
		metaData       common.MetaData
		expectedStatus string
		instanceID     int
		chunk1         []byte
		chunk2         []byte
		offset         int64
		data           string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 5, ChunkSize: 4096, InstanceID: 12, DataID: 12},
			common.PartiallyReceived, 12, data1, nil, 0, "hello"},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 13, DataID: 13,
			DestID: "dev1", DestType: "device", NoData: true, OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 13, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "21", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 13, DataID: 0,
			DestID: "dev1", DestType: "device", MetaOnly: true, OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 13, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "22", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 13, DataID: 13,
			DestID: "dev1", DestType: "device", MetaOnly: true, OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.PartiallyReceived, 13, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 15, DataID: 15,
			DestinationsList: []string{"device:dev1", "device2:dev", "device2:dev1"},
			Link:             "true", OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 15, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 16, DataID: 16,
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 12, ChunkSize: 5},
			common.PartiallyReceived, 16, data2, data3, 5, "hello world!"},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "someorg", InstanceID: 17, DataID: 17,
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.PartiallyReceived, 17, data4, nil, 0, ""},
	}

	// TODO: add instance ID checks
	for _, row := range tests {

		// The receiving side
		// Object update
		if err := handleUpdate(row.metaData, 1); err != nil {
			t.Errorf("handleUpdate failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		// Retrieve object's meta data and status, and check them
		status, err := Store.RetrieveObjectStatus(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to fetch updated object's status (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		if status != row.expectedStatus {
			t.Errorf("Wrong status: %s instead of %s (objectID = %s)", status, row.expectedStatus, row.metaData.ObjectID)
		}
		notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err != nil && !storage.IsNotFound(err) {
			t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else {
			if notification == nil {
				t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
			} else if notification.Status != common.Getdata && status == common.PartiallyReceived {
				t.Errorf("Wrong notification status: %s instead of getdata (objectID = %s)", notification.Status,
					row.metaData.ObjectID)
			} else if notification.Status != common.Received && status == common.CompletelyReceived {
				t.Errorf("Wrong notification status: %s instead of received (objectID = %s)", notification.Status,
					row.metaData.ObjectID)
			}
		}

		// Data
		if row.chunk1 != nil {
			if _, err := handleData(row.chunk1); err != nil {
				t.Errorf("handleData failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else if row.chunk2 != nil {
				// Check notification
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.OriginType, row.metaData.OriginID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
					} else {
						if notification.Status != common.Getdata {
							t.Errorf("Wrong notification status: %s instead of getdata (objectID = %s)", notification.Status,
								row.metaData.ObjectID)
						}
						id := common.GetNotificationID(*notification)
						chunksInfo, ok := notificationChunks[id]
						if !ok {
							t.Errorf("No chunks info (objectID = %s)", row.metaData.ObjectID)
						} else {
							if chunksInfo.maxRequestedOffset != row.offset {
								t.Errorf("Wrong max requested offset in chunks info: %d instead of %d (objectID = %s)", chunksInfo.maxRequestedOffset,
									row.offset, row.metaData.ObjectID)
							}
							if chunksInfo.maxReceivedOffset != 0 {
								t.Errorf("Wrong max received offset in chunks info: %d instead of 0 (objectID = %s)", chunksInfo.maxReceivedOffset,
									row.metaData.ObjectID)
							}
							if _, ok := chunksInfo.chunkResendTimes[chunksInfo.maxRequestedOffset]; !ok {
								t.Errorf("No resend time for offset = %d in chunks info (objectID = %s)", chunksInfo.maxReceivedOffset,
									row.metaData.ObjectID)
							}
							if len(chunksInfo.chunksReceived) != 1 {
								t.Errorf("Wrong chunksReceived array size: %d instead of 1 (objectID = %s)", len(chunksInfo.chunksReceived),
									row.metaData.ObjectID)
							} else if chunksInfo.chunksReceived[0] != 1 {
								t.Errorf("Wrong chunksReceived entry: %d instead of 1 (objectID = %s)", chunksInfo.chunksReceived[0],
									row.metaData.ObjectID)
							}
							if chunksInfo.resendTime == 0 {
								t.Errorf("Resend time not set (objectID = %s)", row.metaData.ObjectID)
							}
						}
					}
				}
				// Get another chunk
				if _, err := handleData(row.chunk2); err != nil {
					t.Errorf("handleData failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					id := common.GetNotificationID(*notification)
					if _, ok := notificationChunks[id]; ok {
						t.Errorf("chunksInfo exists after all data was received (objectID = %s)", row.metaData.ObjectID)
					}
				}
			}
			// Check status: should change to completely received
			storedStatus, err := Store.RetrieveObjectStatus(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
			if err != nil {
				t.Errorf("Failed to fetch updated object's status (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else if storedStatus != common.CompletelyReceived {
				t.Errorf("Wrong status: %s instead of completely received (objectID = %s)", storedStatus, row.metaData.ObjectID)
			}
			// Check data
			storedDataReader, err := Store.RetrieveObjectData(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID, false)
			if err != nil {
				t.Errorf("Failed to fetch object's data (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				if storedDataReader == nil {
					if row.data != "" {
						t.Errorf("RetrieveObjectData returned nil data reader (objectID = %s)", row.metaData.ObjectID)
					}
				} else {
					storedData := make([]byte, 100)
					n, err := storedDataReader.Read(storedData)
					if err != nil {
						t.Errorf("Failed to read object's data from the returned reader (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
					}
					if n != len(row.data) {
						t.Errorf("getObjectData read incorrect data size 's data from the returned reader (objectID = %s): %d instead of %d",
							row.metaData.ObjectID, n, len(row.data))
					}
					storedData = storedData[:n]
					if string(storedData) != string(row.data) {
						t.Errorf("getObjectData returned incorrect data (objectID = %s): %s instead of %s", row.metaData.ObjectID,
							string(storedData), string(row.data))
					}
				}
			}
			// There should be a "received" notification
			notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				row.metaData.OriginType, row.metaData.OriginID)
			if err != nil || notification == nil {
				t.Errorf("RetrieveNotificationRecord failed (objectID = %s). Error: %s ", row.metaData.ObjectID, err.Error())
			} else if notification.Status != common.Received {
				t.Errorf("Wrong notification status: %s instead of received (objectID = %s)", notification.Status,
					row.metaData.ObjectID)
			}
			notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				row.metaData.DestType, row.metaData.DestID)
			if err == nil && notification != nil {
				t.Errorf("Found notification for completely received object (objectID = %s)", row.metaData.ObjectID)
			}
		}

		// "Send" received message and handle ackreceived message
		// TODO: test for ESS too
		notificationsInfo, err := PrepareObjectStatusNotification(row.metaData, common.Received)
		if err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		err = SendNotifications(notificationsInfo)
		if err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		if err := handleAckObjectReceived(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
			t.Errorf("handleAckObjectReceived failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}

		// Delete
		if err := handleDelete(row.metaData); err != nil {
			t.Errorf("handleDelete failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		// Should be marked as deleted
		status, err = Store.RetrieveObjectStatus(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
		if err != nil {
			t.Errorf("RetrieveObject failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else if status != common.ObjDeleted {
			t.Errorf("Object is not marked as deleted (objectID = %s)", row.metaData.ObjectID)
		}

		// There should be no data
		dataReader, _ := Store.RetrieveObjectData(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID, false)
		if dataReader != nil {
			t.Errorf("Deleted object has data (objectID = %s)", row.metaData.ObjectID)
		}

		// There should be no notifications
		notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err == nil && notification != nil {
			t.Errorf("Found notification for deleted object (objectID = %s)", row.metaData.ObjectID)
		}
		notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.DestType, row.metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Found notification for deleted object (objectID = %s)", row.metaData.ObjectID)
		}
	}

	for _, row := range tests {
		if row.metaData.MetaOnly {
			// Can't update object's meta, since the object was deleted
			continue
		}
		// Object update
		if err := handleUpdate(row.metaData, 1); err != nil {
			t.Errorf("handleUpdate failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}

		// "Send" consumed message and handle ackconsumed message
		// TODO: test for ESS too
		notificationsInfo, err := PrepareObjectStatusNotification(row.metaData, common.Consumed)
		if err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		err = SendNotifications(notificationsInfo)
		if err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		if err := handleAckConsumed(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
			t.Errorf("handleAckConsumed failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}

		// There should be no object
		object, err := Store.RetrieveObject(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
		if err == nil && object != nil {
			t.Errorf("Found consumed object (objectID = %s)", row.metaData.ObjectID)
		}

		// There should be no notifications
		notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err == nil && notification != nil {
			t.Errorf("Found notification for deleted object (objectID = %s)", row.metaData.ObjectID)
		}
	}

	destination1 := common.Destination{DestOrgID: "someorg", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := Store.StoreDestination(destination1); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}
	destination2 := common.Destination{DestOrgID: "someorg", DestType: "device2", DestID: "dev", Communication: common.MQTTProtocol}
	if err := Store.StoreDestination(destination2); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}
	destination3 := common.Destination{DestOrgID: "someorg", DestType: "device2", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := Store.StoreDestination(destination3); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	for _, row := range tests {
		// The sending side
		if _, err := Store.StoreObject(row.metaData, row.chunk1, common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else {
			notificationsInfo, err := PrepareObjectNotifications(row.metaData)
			if err != nil {
				t.Errorf("Failed to send object notifications (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			}
			err = SendNotifications(notificationsInfo)
			if err != nil {
				t.Errorf("Failed to send object notifications (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			}

			// Updated
			destType := row.metaData.DestType
			destID := row.metaData.DestID
			if destType == "" {
				destType = destination1.DestType
				destID = destination1.DestID
			}
			if err = handleObjectUpdated(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				destType, destID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
				t.Errorf("handleObjectUpdated failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					destType, destID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

					} else {
						if notification.Status != common.Updated {
							t.Errorf("Wrong notification status: %s instead of updated (objectID = %s)", notification.Status,
								row.metaData.ObjectID)
						}
					}
				}
			}

			// Get data
			if row.metaData.DestType != "" {
				// Can't check handleGetData with destinations list
				if err := handleGetData(row.metaData, row.metaData.InstanceID); err != nil {
					t.Errorf("handleGetData failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
						destType, destID)
					if err != nil && !storage.IsNotFound(err) {
						t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
					} else {
						if notification == nil {
							t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

						} else {
							if notification.Status != common.Data {
								t.Errorf("Wrong notification status: %s instead of data (objectID = %s)", notification.Status,
									row.metaData.ObjectID)
							}
						}
					}
				}
			}

			// Received
			if err := handleObjectReceived(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				destType, destID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
				t.Errorf("handleObjectReceived failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					destType, destID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

					} else {
						if notification.Status != common.ReceivedByDestination {
							t.Errorf("Wrong notification status: %s instead of ReceivedByDestination (objectID = %s)", notification.Status,
								row.metaData.ObjectID)
						}
					}
				}
			}

			// Consumed
			if err := handleObjectConsumed(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				destType, destID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
				t.Errorf("handleObjectConsumed failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					destType, destID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

					} else {
						if notification.Status != common.ConsumedByDestination {
							t.Errorf("Wrong notification status: %s instead of ConsumedByDestination (objectID = %s)",
								notification.Status, row.metaData.ObjectID)
						}
					}
				}
			}

			// Resend
			if err := handleResendRequest(destination1); err != nil {
				t.Errorf("handleResendRequest failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					destination1.DestType, destination1.DestID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

					} else {
						if notification.Status != common.Update {
							t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status,
								row.metaData.ObjectID)
						}
					}
				}
			}

			// Delete
			if !row.metaData.MetaOnly {
				// Don't check for metaOnly because it can't be restored

				// "Send" delete notification and handle the ack
				notification := common.Notification{ObjectID: row.metaData.ObjectID, ObjectType: row.metaData.ObjectType,
					DestOrgID: row.metaData.DestOrgID, DestID: destination1.DestID, DestType: destination1.DestType,
					Status: common.Delete, InstanceID: row.metaData.InstanceID}
				if err := Store.UpdateNotificationRecord(notification); err != nil {
					t.Errorf("UpdateNotificationRecord failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if err := handleAckDelete(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
						destination1.DestType, destination1.DestID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
						t.Errorf("handleAckDelete failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
					} else {
						notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
							destination1.DestType, destination1.DestID)
						if err != nil && !storage.IsNotFound(err) {
							t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
						} else {
							if notification == nil {
								t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
							} else {
								if notification.Status != common.AckDelete {
									t.Errorf("Wrong notification status: %s instead of ackdelete (objectID = %s)", notification.Status,
										row.metaData.ObjectID)
								}
							}
						}

						storedObject, _ := Store.RetrieveObject(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
						if row.metaData.DestinationsList == nil && storedObject != nil {
							t.Errorf("Object exists after ackdelete received (objectID = %s)", row.metaData.ObjectID)
						} else if row.metaData.DestinationsList != nil && storedObject == nil {
							t.Errorf("Object deleted by only one destination was deleted (objectID = %s)", row.metaData.ObjectID)
						}
					}

					if row.metaData.DestinationsList != nil {
						// This object has three destinations and has to be deleted only after receiving ackDelete from all the three
						notification = common.Notification{ObjectID: row.metaData.ObjectID, ObjectType: row.metaData.ObjectType,
							DestOrgID: row.metaData.DestOrgID, DestID: destination2.DestID, DestType: destination2.DestType,
							Status: common.Delete, InstanceID: row.metaData.InstanceID}
						if err := Store.UpdateNotificationRecord(notification); err != nil {
							t.Errorf("UpdateNotificationRecord failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
						}
						notification = common.Notification{ObjectID: row.metaData.ObjectID, ObjectType: row.metaData.ObjectType,
							DestOrgID: row.metaData.DestOrgID, DestID: destination3.DestID, DestType: destination3.DestType,
							Status: common.Delete, InstanceID: row.metaData.InstanceID}
						if err := Store.UpdateNotificationRecord(notification); err != nil {
							t.Errorf("UpdateNotificationRecord failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
						}
						if err := handleAckDelete(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
							destination2.DestType, destination2.DestID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
							t.Errorf("handleAckDelete failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
						} else {
							storedObject, _ := Store.RetrieveObject(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
							if storedObject == nil {
								t.Errorf("Object deleted by only one destination was deleted (objectID = %s)", row.metaData.ObjectID)
							}
							if err := handleAckDelete(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
								destination3.DestType, destination3.DestID, row.metaData.InstanceID, row.metaData.DataID); err != nil {
								t.Errorf("handleAckDelete failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
							} else {
								storedObject, _ := Store.RetrieveObject(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
								if storedObject != nil {
									t.Errorf("Object exists after ackdelete received from all destinations (objectID = %s)", row.metaData.ObjectID)
								}
							}
						}
					}
				}

				if _, err := Store.StoreObject(row.metaData, row.chunk1, common.ReadyToSend); err != nil {
					t.Errorf("Failed to store object (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				}
			}
		}
	}

	if err := Store.DeleteNotificationRecords("someorg", "", "", "", ""); err != nil {
		t.Errorf("Failed to delete notifications. Error: %s", err.Error())
	}
	for _, row := range tests {

		if err := storage.DeleteStoredObject(Store, row.metaData); err != nil {
			t.Errorf("Failed to delete object. Error: %s", err.Error())
		}
	}
}

func TestPingAndRegisterNew(t *testing.T) {
	testPingAndRegisterNew(common.Bolt, t)
	testPingAndRegisterNew(common.Mongo, t)
}

func testPingAndRegisterNew(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS

	var err error
	Store, err = setUpStorage(storageType)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start communication. Error: %s", err.Error())
	}

	tests := []struct {
		metaData common.MetaData
		status   string
		data     []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "pingorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.ReadyToSend, []byte("hello")},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "pingorg",
			DestID: "dev1", DestType: "type2", OriginID: "123", OriginType: "type2"},
			common.ReadyToSend, []byte("hello")},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "pingorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.NotReadyToSend, nil},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "pingorg",
			OriginID: "123", OriginType: "type2", DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
			}},
			common.ReadyToSend, []byte("hello")},
	}

	for _, test := range tests {
		if _, err := Store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("StoreObject failed. Error: %s", err.Error())
		}
		if err := Store.DeleteNotificationRecords(test.metaData.DestOrgID, test.metaData.ObjectType,
			test.metaData.ObjectID, "", ""); err != nil {
			t.Errorf("DeleteNotificationRecords failed. Error: %s", err.Error())
		}
	}

	dest := common.Destination{DestOrgID: "pingorg", DestType: "device", DestID: "dev1",
		Communication: common.MQTTProtocol}
	if err := Store.DeleteDestination(dest.DestOrgID, dest.DestType, dest.DestID); err != nil {
		t.Errorf("DeleteDestination failed. Error: %s", err.Error())
	}

	if err := handlePing(dest); err == nil {
		t.Errorf("handlePing for non-existing destination succeeded")
	} else {
		if !isIgnoredByHandler(err) {
			t.Errorf("handlePing failed. Error: %s", err.Error())
		}
		notification, err := Store.RetrieveNotificationRecord(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Notification for non-existing destination (objectID = %s)", tests[0].metaData.ObjectID)
		}
	}

	if err := handleRegisterNew(dest, false); err != nil {
		t.Errorf("handleRegisterNew failed. Error: %s", err.Error())
	} else {
		notification, err := Store.RetrieveNotificationRecord(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID)
		if err != nil && !storage.IsNotFound(err) {
			t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", tests[0].metaData.ObjectID, err.Error())
		} else {
			if notification == nil {
				t.Errorf("No notification record (objectID = %s)", tests[0].metaData.ObjectID)
			} else if notification.Status != common.Update {
				t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status,
					tests[0].metaData.ObjectID)
			}
		}

		notification, err = Store.RetrieveNotificationRecord(tests[1].metaData.DestOrgID, tests[1].metaData.ObjectType,
			tests[1].metaData.ObjectID, tests[1].metaData.DestType, tests[1].metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Created notification for object with another destination")
		}

		notification, err = Store.RetrieveNotificationRecord(tests[2].metaData.DestOrgID, tests[2].metaData.ObjectType,
			tests[2].metaData.ObjectID, tests[2].metaData.DestType, tests[2].metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Created notification for NotReadyToSend object")
		}

		notification, err = Store.RetrieveNotificationRecord(tests[3].metaData.DestOrgID, tests[3].metaData.ObjectType,
			tests[3].metaData.ObjectID, dest.DestType, dest.DestID)
		if err == nil && notification != nil {
			t.Errorf("Created notification for object with policy")
		}

		storedDests, err := Store.GetObjectDestinationsList(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID)
		if err != nil {
			t.Errorf("GetObjectDestinationsList failed. Error: %s", err.Error())
		} else if len(storedDests) != 1 {
			t.Errorf("GetObjectDestinationsList returned %d destinations instead of 1.", len(storedDests))
		} else {
			if storedDests[0].Destination != dest {
				t.Errorf("GetObjectDestinationsList returned incorrect destination.")
			}
			if storedDests[0].Status != common.Delivering {
				t.Errorf("GetObjectDestinationsList returned destination in a wrong status: %s instead of delivering.",
					storedDests[0].Status)
			}
		}
	}

	if err := Store.DeleteNotificationRecords(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
		tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID); err != nil {
		t.Errorf("DeleteNotificationRecords failed. Error: %s", err.Error())
	}
	if _, err := Store.UpdateObjectDeliveryStatus(common.Delivered, "", tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
		tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID); err != nil {
		t.Errorf("UpdateObjectDeliveryStatus failed. Error: %s", err.Error())
	}

	// Send ping from a registered ESS
	if err := handlePing(dest); err != nil {
		t.Errorf("handlePing failed. Error: %s", err.Error())
	} else {
		notification, err := Store.RetrieveNotificationRecord(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Created notification for existing destination")
		}
	}

	if err := Store.DeleteDestination(dest.DestOrgID, dest.DestType, dest.DestID); err != nil {
		t.Errorf("DeleteDestination failed. Error: %s", err.Error())
	}
	// Destination is not registered but there is a destination in the object in delivered status
	if err := handleRegisterNew(dest, true); err != nil {
		t.Errorf("handlePing handleRegisterNew. Error: %s", err.Error())
	} else {
		notification, err := Store.RetrieveNotificationRecord(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID)
		if err == nil && notification != nil {
			t.Errorf("Created notification for delivered object for persistent destination")
		}
	}

	if err := Store.DeleteDestination(dest.DestOrgID, dest.DestType, dest.DestID); err != nil {
		t.Errorf("DeleteDestination failed. Error: %s", err.Error())
	}
	// Destination is not registered but there is a destination in the object in delivered status
	if err := handleRegisterNew(dest, false); err != nil {
		t.Errorf("handleRegisterNew failed. Error: %s", err.Error())
	} else {
		notification, err := Store.RetrieveNotificationRecord(tests[0].metaData.DestOrgID, tests[0].metaData.ObjectType,
			tests[0].metaData.ObjectID, tests[0].metaData.DestType, tests[0].metaData.DestID)
		if err != nil && !storage.IsNotFound(err) {
			t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", tests[0].metaData.ObjectID, err.Error())
		} else {
			if notification == nil {
				t.Errorf("No notification record (objectID = %s)", tests[0].metaData.ObjectID)
			} else if notification.Status != common.Update {
				t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status,
					tests[0].metaData.ObjectID)
			}
		}
	}
}

func TestRegisterAsNew(t *testing.T) {
	testRegisterAsNew(common.Bolt, t)
	testRegisterAsNew(common.InMemory, t)
}

func testRegisterAsNew(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.ESS

	var err error
	Store, err = setUpStorage(storageType)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start communication. Error: %s", err.Error())
	}

	tests := []struct {
		metaData           common.MetaData
		status             string
		data               []byte
		notificationStatus string
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "regorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.CompletelyReceived, []byte("hello"), ""},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "regorg",
			DestID: "dev1", DestType: "type2", OriginID: "123", OriginType: "type2"},
			common.PartiallyReceived, []byte("hello"), ""},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "regorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.ReadyToSend, nil, common.Update},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "regorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.ReadyToSend, nil, common.Update},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "regorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2"},
			common.NotReadyToSend, nil, ""},
	}

	for _, test := range tests {
		if _, err := Store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("StoreObject failed. Error: %s", err.Error())
		}

		if test.notificationStatus != "" {
			notification := common.Notification{ObjectID: test.metaData.ObjectID, ObjectType: test.metaData.ObjectType,
				DestOrgID: test.metaData.DestOrgID, DestID: test.metaData.DestID, DestType: test.metaData.DestType,
				Status: test.notificationStatus, InstanceID: 0}

			if err := Store.UpdateNotificationRecord(notification); err != nil {
				t.Errorf("UpdateNotificationRecord failed. Error: %s", err.Error())
			}
		}
	}

	if err := handleRegisterAsNew(); err != nil {
		t.Errorf("handleRegisterAsNew failed. Error: %s", err.Error())
	} else {
		if !registerAsNew {
			t.Errorf("registerAsNew is false after handleRegisterAsNew")
		}
		if common.Registered {
			t.Errorf("Registered is true after handleRegisterAsNew")
		}
		for _, test := range tests {
			object, err := Store.RetrieveObject(test.metaData.DestOrgID, test.metaData.ObjectType,
				test.metaData.ObjectID)
			if err != nil && !common.IsNotFound(err) {
				t.Errorf("RetrieveObject failed (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
			} else {
				switch test.status {
				case common.PartiallyReceived:
					if object != nil {
						t.Errorf("Object (objectID = %s) was not deleted", test.metaData.ObjectID)
					}

				case common.CompletelyReceived:
					if Store.IsPersistent() && object == nil {
						t.Errorf("Object (objectID = %s) was deleted", test.metaData.ObjectID)
					}
					if !Store.IsPersistent() && object != nil {
						t.Errorf("Object (objectID = %s) was not deleted", test.metaData.ObjectID)
					}

				case common.ReadyToSend:
					if object == nil {
						t.Errorf("Object (objectID = %s) was deleted", test.metaData.ObjectID)
					}

				case common.NotReadyToSend:
					if object == nil {
						t.Errorf("Object (objectID = %s) was deleted", test.metaData.ObjectID)
					}
				}

				notification, err := Store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType,
					test.metaData.ObjectID, test.metaData.DestType, test.metaData.DestID)
				if err == nil && notification != nil {
					t.Errorf("Notification exists (objectID = %s)", test.metaData.ObjectID)
				}
			}
		}
	}

	handleRegAck()
	if registerAsNew {
		t.Errorf("registerAsNew is true after handleRegAck")
	}
	if !common.Registered {
		t.Errorf("Registered is false after handleRegAck")
	}
	for _, test := range tests {
		if test.status == common.ReadyToSend {
			notification, err := Store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType,
				test.metaData.ObjectID, test.metaData.DestType, test.metaData.DestID)
			if err != nil && !storage.IsNotFound(err) {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No notification record (objectID = %s)", test.metaData.ObjectID)
				} else if notification.Status != common.Update {
					t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status,
						test.metaData.ObjectID)
				}
			}
		}
	}
}

func TestRegisterDestinationValidity(t *testing.T) {
	common.Configuration.NodeType = common.ESS

	var err error
	Store, err = setUpStorage(common.Mongo)
	if err != nil {
		t.Errorf("%s", err.Error())
		return
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start communication. Error: %s", err.Error())
	}

	tests := []struct {
		dest common.Destination
	}{
		{common.Destination{DestOrgID: "testorg", DestType: "device%", DestID: "dev1", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "testorg", DestType: "device", DestID: "dev1//", Communication: common.MQTTProtocol}},
		{common.Destination{DestOrgID: "testorg", DestType: "device[", DestID: "dev1}", Communication: common.MQTTProtocol}},
	}

	for _, test := range tests {
		if err := handleRegisterNew(test.dest, false); err == nil {
			t.Errorf("handleRegisterNew registered destination with invalid type (%s) or id (%s)", test.dest.DestType, test.dest.DestID)
		}
		if err := handleRegistration(test.dest, false); err == nil {
			t.Errorf("handleRegistration handled destination with invalid type (%s) or id (%s)", test.dest.DestType, test.dest.DestID)
		}
		if err := handlePing(test.dest); err == nil {
			t.Errorf("handlePing handled destination with invalid type (%s) or id (%s)", test.dest.DestType, test.dest.DestID)
		}
	}
}

func setUpStorage(storageType string) (storage.Storage, error) {
	var store storage.Storage
	if storageType == common.InMemory {
		store = &storage.Cache{Store: &storage.InMemoryStorage{}}
	} else if storageType == common.Bolt {
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup(true)
		store = &storage.Cache{Store: boltStore}
	} else {
		common.Configuration.MongoDbName = "d_test_db"
		store = &storage.Cache{Store: &storage.MongoStorage{}}
	}
	if err := store.Init(); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())}
	}
	return store, nil
}
