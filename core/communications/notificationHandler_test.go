package communications

import (
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func TestNotificationHandler(t *testing.T) {

	if common.Registered {
		t.Errorf("Registered flag is true")
	} else {
		handleRegAck()
		if !common.Registered {
			t.Errorf("Registered flag is false after calling handleRegAck")
		}
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
	boltStor := &storage.BoltStorage{}
	boltStor.Cleanup()
	Store = boltStor
	if err := Store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer Store.Stop()

	Comm = &TestComm{}
	if err := Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	common.Configuration.NodeType = common.CSS
	if err := handleRegistration(dest, false); err != nil {
		t.Errorf("Failed to register destination. Error: %s", err.Error())
	}

	if err := handleRegistration(dest, false); err != nil {
		t.Errorf("Failed to re-register destination. Error: %s", err.Error())
	}

	data1 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, common.Version,
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '1',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, dataField, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o',
	}
	data2 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, common.Version,
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '4',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, dataField, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o',
	}
	data3 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, common.Version,
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '4',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, dataField, 0, 0, 0, 7, ' ', 'w', 'o', 'r', 'l', 'd', '!',
	}
	data4 := []byte{1, 1, 1, 1, // magic
		0, 0, 0, common.Version,
		0, 0, 0, fieldCount,
		0, 0, 0, orgIDField, 0, 0, 0, 7, 's', 'o', 'm', 'e', 'o', 'r', 'g',
		0, 0, 0, objectTypeField, 0, 0, 0, 5, 't', 'y', 'p', 'e', '1',
		0, 0, 0, objectIDField, 0, 0, 0, 1, '5',
		0, 0, 0, offsetField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, instanceIDField, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, dataField, 0, 0, 0, 0,
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
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 5, ChunkSize: 4096},
			common.PartiallyReceived, 0, data1, nil, 0, "hello"},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", NoData: true, OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 0, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", MetaOnly: true, OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 0, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", Link: "true", OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.CompletelyReceived, 0, nil, nil, 0, ""},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 12, ChunkSize: 5},
			common.PartiallyReceived, 0, data2, data3, 5, "hello world!"},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "someorg",
			DestID: "dev1", DestType: "device", OriginID: "123", OriginType: "type2", ObjectSize: 0, ChunkSize: 4096},
			common.PartiallyReceived, 0, data4, nil, 0, ""},
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
				if status == common.PartiallyReceived {
					t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)
				}
			} else {
				if status == common.CompletelyReceived {
					t.Errorf("Notification record created for completely received object (objectID = %s): %#v", row.metaData.ObjectID, notification)
				} else {
					if notification.Status != common.Getdata {
						t.Errorf("Wrong notification status: %s instead of getdata (objectID = %s)", notification.Status,
							row.metaData.ObjectID)
					}
				}
			}
		}

		// Data

		// To handle chunked data CSS has to be the leader, we switch here to ESS to avoid this problem.
		common.Configuration.NodeType = common.ESS

		if row.chunk1 != nil {
			if err := handleData(row.chunk1); err != nil {
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
				if err := handleData(row.chunk2); err != nil {
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
			storedDataReader, err := Store.RetrieveObjectData(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
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
			if err == nil || notification != nil {
				t.Errorf("Found notification for completely received object (objectID = %s)", row.metaData.ObjectID)
			}
		}

		// To handle chunked data CSS has to be the leader, we switched to ESS before to avoid this problem. Switching back to CSS.
		common.Configuration.NodeType = common.CSS

		// "Send" received message and handle ackreceived message
		// TODO: test for ESS too
		if err := SendObjectStatus(row.metaData, common.Received); err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		if err := handleAckObjectReceived(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID, 0); err != nil {
			t.Errorf("handleAckObjectReceived failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}

		// "Send" consumed message and handle ackconsumed message
		// TODO: test for ESS too
		if err := SendObjectStatus(row.metaData, common.Consumed); err != nil {
			t.Errorf("SendObjectStatus failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		}
		if err := handleAckConsumed(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID, 0); err != nil {
			t.Errorf("handleAckConsumed failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
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
		dataReader, _ := Store.RetrieveObjectData(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID)
		if dataReader != nil {
			t.Errorf("Deleted object has data (objectID = %s)", row.metaData.ObjectID)
		}

		// There should be no notifications
		notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.OriginType, row.metaData.OriginID)
		if err == nil || notification != nil {
			t.Errorf("Found notification for deleted object (objectID = %s)", row.metaData.ObjectID)
		}
		notification, err = Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
			row.metaData.DestType, row.metaData.DestID)
		if err == nil || notification != nil {
			t.Errorf("Found notification for deleted object (objectID = %s)", row.metaData.ObjectID)
		}
	}

	destination := common.Destination{DestOrgID: "someorg", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := Store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}
	for _, row := range tests {
		// The sending side
		if err := Store.StoreObject(row.metaData, row.chunk1, common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else if err := SendObjectNotifications(row.metaData); err != nil {
			t.Errorf("Failed to send object notifications (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
		} else {

			// Updated
			if err = handleObjectUpdated(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				row.metaData.DestType, row.metaData.DestID, 0); err != nil {
				t.Errorf("handleObjectUpdated failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.DestType, row.metaData.DestID)
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
			if err := handleGetData(row.metaData, 0); err != nil {
				t.Errorf("handleGetData failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.DestType, row.metaData.DestID)
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

			// Received
			if err := handleObjectReceived(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
				row.metaData.DestType, row.metaData.DestID, 0); err != nil {
				t.Errorf("handleObjectReceived failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.DestType, row.metaData.DestID)
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
				row.metaData.DestType, row.metaData.DestID, 0); err != nil {
				t.Errorf("handleObjectConsumed failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.DestType, row.metaData.DestID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						t.Errorf("No notification record (objectID = %s)", row.metaData.ObjectID)

					} else {
						if notification.Status != common.AckConsumed {
							t.Errorf("Wrong notification status: %s instead of ackconsumed (objectID = %s)", notification.Status,
								row.metaData.ObjectID)
						}
					}
				}
			}

			// Resend
			if err := handleResendRequest(destination); err != nil {
				t.Errorf("handleResendRequest failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
			} else {
				notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
					row.metaData.DestType, row.metaData.DestID)
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
					DestOrgID: row.metaData.DestOrgID, DestID: destination.DestID, DestType: destination.DestType,
					Status: common.Delete, InstanceID: 0}
				if err := Store.UpdateNotificationRecord(notification); err != nil {
					t.Errorf("UpdateNotificationRecord failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				} else {
					if err := handleAckDelete(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
						row.metaData.DestType, row.metaData.DestID, 0); err != nil {
						t.Errorf("handleAckDelete failed (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
					} else {
						notification, err := Store.RetrieveNotificationRecord(row.metaData.DestOrgID, row.metaData.ObjectType, row.metaData.ObjectID,
							row.metaData.DestType, row.metaData.DestID)
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
						if storedObject != nil {
							t.Errorf("Object exists after ackdelete received (objectID = %s)", row.metaData.ObjectID)
						}
					}
				}

				if err := Store.StoreObject(row.metaData, row.chunk1, common.ReadyToSend); err != nil {
					t.Errorf("Failed to store object (objectID = %s). Error: %s", row.metaData.ObjectID, err.Error())
				}
			}
		}
	}

	if err := Store.DeleteNotificationRecords("", "", "", "", ""); err != nil {
		t.Errorf("Failed to delete notifications. Error: %s", err.Error())
	}
}
