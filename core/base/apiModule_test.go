package base

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash"
	"math"
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func setupDB(dbType string) {
	if dbType == common.Mongo {
		common.Configuration.MongoDbName = "d_test_db"
		store = &storage.MongoStorage{}
	} else if dbType == common.Bolt {
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup(true)
		store = boltStore
	} else {
		store = &storage.InMemoryStorage{}
	}
}

func setupDataSignature(data []byte, hashAlgo string) (string, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	publicKey := &privateKey.PublicKey
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", "", err
	}
	publicKeyString := base64.StdEncoding.EncodeToString(publicKeyBytes)

	var dataHash hash.Hash
	var cyrptoHash crypto.Hash
	if hashAlgo == common.Sha1 {
		dataHash = sha1.New()
		cyrptoHash = crypto.SHA1
	} else {
		dataHash = sha256.New()
		cyrptoHash = crypto.SHA256
	}

	_, err = dataHash.Write(data)
	if err != nil {
		return "", "", err
	}
	dataHashSum := dataHash.Sum(nil)

	signature, err := rsa.SignPSS(rand.Reader, privateKey, cyrptoHash, dataHashSum, nil)
	if err != nil {
		return "", "", err
	}
	signatureString := base64.StdEncoding.EncodeToString(signature)
	return publicKeyString, signatureString, nil
}

func TestObjectAPI(t *testing.T) {
	setupDB(common.Mongo)
	testObjectAPI(store, t)

	setupDB(common.Bolt)
	testObjectAPI(store, t)
}

func testObjectAPI(store storage.Storage, t *testing.T) {
	communications.Store = store
	common.InitObjectLocks()

	dests := []string{"device:dev1", "device2:dev", "device2:dev1"}

	// set up data signature value
	dataToSign := []byte("data to check signature")
	var publicKeySha1, signatureSha1, publicKeySha256, signatureSha256 string
	var err error
	if publicKeySha1, signatureSha1, err = setupDataSignature(dataToSign, common.Sha1); err != nil {
		t.Errorf("Failed to set up publicKey and signature with SHA1 for data. Error: %s\n", err.Error())
	}

	if publicKeySha256, signatureSha256, err = setupDataSignature(dataToSign, common.Sha256); err != nil {
		t.Errorf("Failed to set up publicKey and signature with SHA256 for data. Error: %s\n", err.Error())
	}

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
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777", MetaOnly: true}, []byte("data"),
			"updateObject didn't check that the data is empty for meta only update"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777", PublicKey: "123"}, []byte("data"),
			"updateObject didn't check that the publicKey is base64 encoded"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777", Signature: "123"}, []byte("data"),
			"updateObject didn't check that the signature is base64 encoded"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777", HashAlgorithm: "123"}, []byte("data"),
			"updateObject didn't check that the hashAlgorithm is SHA1 or SHA256"},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777",
				HashAlgorithm: common.Sha1,
				PublicKey:     publicKeySha1,
				Signature:     signatureSha1,
			},
			[]byte("wrong data to check signature"),
			"updateObject didn't verify the data with publicKey and signature"},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777",
				HashAlgorithm: common.Sha256,
				PublicKey:     publicKeySha256,
				Signature:     signatureSha256,
			},
			[]byte("wrong data to check signature"),
			"updateObject didn't verify the data with publicKey and signature"},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "12345", ObjectType: "type1", DestOrgID: "myorg777",
				HashAlgorithm: common.Sha256,
				PublicKey:     publicKeySha1,
				Signature:     signatureSha1,
			},
			dataToSign,
			"updateObject didn't verify the data with specified hash algorithm"},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Properties: []common.PolicyProperty{
						{Value: float64(1)},
					},
					Constraints: []string{"Plover=34", "asdf=true"},
				},
			}, nil, "UpdateObject didn't check that the property name was missing",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Properties: []common.PolicyProperty{
						{Name: "", Value: float64(1)},
					},
					Constraints: []string{"Plover=34", "asdf=true"},
				},
			}, nil, "UpdateObject didn't check that the property name was empty",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Services: []common.ServiceID{
						{OrgID: "plover", Arch: "amd64", ServiceName: "tester"},
					},
				},
			}, nil, "UpdateObject didn't check that the service ID's version was empty",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Services: []common.ServiceID{
						{OrgID: "plover", Arch: "amd64", Version: "1.0.0"},
					},
				},
			}, nil, "UpdateObject didn't check that the service ID's service name was empty",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Services: []common.ServiceID{
						{OrgID: "plover", ServiceName: "tester", Version: "1.0.0"},
					},
				},
			}, nil, "UpdateObject didn't check that the service ID's architecture was empty",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Services: []common.ServiceID{
						{Arch: "amd64", ServiceName: "tester", Version: "1.0.0"},
					},
				},
			}, nil, "UpdateObject didn't check that the service ID's orgID was empty",
		},
		{"myorg777", "type1", "123456",
			common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Services: []common.ServiceID{
						{OrgID: "plover", Arch: "amd64", ServiceName: "tester", Version: "[1.0.0"},
					},
				},
			}, nil, "UpdateObject didn't check that the service ID's version was invalid",
		},
		{"myorg%777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg%777", MetaOnly: true}, nil,
			"updateObject didn't check that the validity of organization id"},
		{"myorg777", "type1&", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1&", DestOrgID: "myorg777", MetaOnly: true}, nil,
			"updateObject didn't check that the validity of object type"},
		{"myorg777", "type1", "123456:", common.MetaData{ObjectID: "123456:", ObjectType: "type1", DestOrgID: "myorg777", MetaOnly: true}, nil,
			"updateObject didn't check that the validity of object id"},
		{"myorg777", "type1", "123456", common.MetaData{ObjectID: "123456", ObjectType: "type1", DestOrgID: "myorg777", MetaOnly: true, DestType: "%^&+"}, nil,
			"updateObject didn't check that the validity of destination type"},
		{"myorg777", "type1", "777", common.MetaData{ObjectID: "777", ObjectType: "type1", DestOrgID: "myorg777",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
			}}, nil, ""},
		{"myorg777", "type1", "777", common.MetaData{ObjectID: "777", ObjectType: "type1", DestOrgID: "myorg777", MetaOnly: true}, nil,
			"updateObject didn't check that the object had a policy and the update doesn't"},
		{"myorg777", "type1", "888", common.MetaData{ObjectID: "888", ObjectType: "type1", DestOrgID: "myorg777", MetaOnly: true}, nil,
			""},
		{"myorg777", "type1", "88", common.MetaData{ObjectID: "888", ObjectType: "type1", DestOrgID: "myorg777",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
			}}, nil, "updateObject didn't check that the object didn't have a policy and the update does"},
	}

	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer store.Stop()

	for _, row := range invalidObjects {
		err := UpdateObject(row.orgID, row.objectType, row.objectID, row.metaData, nil)
		if err == nil && row.message != "" {
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
		updateDests        bool
	}{
		{"myorg777", "type1", "1", common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg777",
			DestID: "dev1", DestType: "device"},
			nil, common.NotReadyToSend, 1, []byte("new"), 1, false},
		{"myorg777", "type1", "2", common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: -1, NoData: true, Link: "abc", DestID: "dev1", DestType: "device"},
			[]byte("abc"), common.ReadyToSend, math.MaxInt32, []byte("new"), 1, false},
		{"myorg777", "type1", "3", common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, Link: "abc", DestID: "dev2", DestType: "device"},
			nil, common.ReadyToSend, 3, []byte("new"), 0, false},
		{"myorg777", "type1", "31", common.MetaData{ObjectID: "31", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, MetaOnly: true, DestID: "dev2", DestType: "device"},
			nil, common.NotReadyToSend, 3, []byte("new"), 0, false},
		{"myorg777", "type1", "4", common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1"},
			[]byte("abc"), common.ReadyToSend, 3, []byte("new"), 1, false},
		{"myorg777", "type1", "5", common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", Inactive: true},
			[]byte("abc"), common.ReadyToSend, 3, []byte("new"), 1, false},
		{"myorg777", "type1", "6", common.MetaData{ObjectID: "6", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 1, DestinationsList: dests},
			[]byte("abc"), common.ReadyToSend, 1, []byte("new"), 3, false},
		{"myorg777", "type1", "6", common.MetaData{ObjectID: "6", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 1, DestType: "device3", DestID: "dev1", MetaOnly: true},
			nil, common.ReadyToSend, 1, []byte("new"), 1, true},
		{"myorg777", "type1", "7", common.MetaData{ObjectID: "7", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", HashAlgorithm: common.Sha1, PublicKey: publicKeySha1, Signature: signatureSha1},
			dataToSign, common.ReadyToSend, 3, nil, 1, false},
		{"myorg777", "type1", "8", common.MetaData{ObjectID: "8", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", HashAlgorithm: common.Sha1, PublicKey: publicKeySha1, Signature: signatureSha1,
			NoData: true},
			dataToSign, common.ReadyToSend, 3, nil, 1, false},
		{"myorg777", "type1", "71", common.MetaData{ObjectID: "71", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", HashAlgorithm: common.Sha256, PublicKey: publicKeySha256, Signature: signatureSha256},
			dataToSign, common.ReadyToSend, 3, nil, 1, false},
		{"myorg777", "type1", "81", common.MetaData{ObjectID: "81", ObjectType: "type1", DestOrgID: "myorg777",
			ExpectedConsumers: 3, DestType: "device", DestID: "dev1", HashAlgorithm: common.Sha256, PublicKey: publicKeySha256, Signature: signatureSha256,
			NoData: true},
			dataToSign, common.ReadyToSend, 3, nil, 1, false},
	}

	destination1 := common.Destination{DestOrgID: "myorg777", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination1); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination2 := common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination2); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination3 := common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination3); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination4 := common.Destination{DestOrgID: "myorg777", DestType: "device3", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination4); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	communications.Comm = &communications.TestComm{}
	if err := communications.Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	for _, row := range validObjects {
		// Update object
		err := UpdateObject(row.orgID, row.objectType, row.objectID, row.metaData, row.data)
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

		if row.metaData.NoData && (metaData.PublicKey != "" || metaData.Signature != "") {
			t.Errorf("Public key and signature should be empty value if NoData is true (objectID = %s)", row.objectID)
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
			if destination1.DestID == metaData.DestID {
				notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
				if err != nil {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
				}
				if (metaData.DestType == destination1.DestType || metaData.DestType == "") &&
					(metaData.DestID == destination1.DestID || metaData.DestID == "") {
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

		if row.updateDests {
			// There should be delete notifications for destinations 1-3
			notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, destination1.DestType, destination1.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No delete notification record (objectID = %s)", row.objectID)
				} else {
					if notification.Status != common.Delete {
						t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, row.objectID)
					}
				}
			}
			notification, err = store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, destination2.DestType, destination2.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No delete notification record (objectID = %s)", row.objectID)
				} else {
					if notification.Status != common.Delete {
						t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, row.objectID)
					}
				}
			}
			notification, err = store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, destination3.DestType, destination3.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
			} else {
				if notification == nil {
					t.Errorf("No delete notification record (objectID = %s)", row.objectID)
				} else {
					if notification.Status != common.Delete {
						t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, row.objectID)
					}
				}
			}
		}

		// Check other object APIs
		// Get status
		storedStatus, err := GetObjectStatus(row.orgID, row.objectType, row.objectID)
		if err != nil {
			t.Errorf("Failed to get object's status (objectID = %s). Error: %s", row.objectID, err.Error())
		}
		if storedStatus != row.expectedStatus {
			t.Errorf("getObjectStatus returned incorrect status: %s instead of %s", storedStatus, row.expectedStatus)
		}

		// Get data
		if !metaData.MetaOnly {
			storedDataReader, err := GetObjectData(row.orgID, row.objectType, row.objectID)
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
			ok, err := PutObjectData(row.orgID, row.objectType, row.objectID, bytes.NewReader(row.newData))
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
						storedStatus, err := GetObjectStatus(row.orgID, row.objectType, row.objectID)
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

						if destination1.DestID == metaData.DestID {
							notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
							if err != nil {
								t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
							}

							if !metaData.Inactive && (metaData.DestType == destination1.DestType || metaData.DestType == "") &&
								(metaData.DestID == destination1.DestID || metaData.DestID == "") {
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
							} else if metaData.Inactive && notification != nil {
								t.Errorf("Found a notification record after data update with an inactive object (objectID = %s)", row.objectID)
							}
						}
					}
				}
			}
		}

		// Mark consumed (should fail)
		if err := ObjectConsumed(row.orgID, row.objectType, row.objectID); err == nil {
			t.Errorf("objectConsumed marked the sender's object as consumed  (objectID = %s)", row.objectID)
		}

		// Mark deleted (should fail)
		if err := ObjectDeleted("", row.orgID, row.objectType, row.objectID); err == nil {
			t.Errorf("objectDeleted marked the sender's object as deleted  (objectID = %s)", row.objectID)
		}

		// Activate
		if err := ActivateObject(row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("Failed to activate object (objectID = %s). Error: %s", row.objectID, err.Error())
		} else {
			if destination1.DestID == metaData.DestID {
				notification, err := store.RetrieveNotificationRecord(row.orgID, row.objectType, row.objectID, metaData.DestType, metaData.DestID)
				if err != nil && !storage.IsNotFound(err) {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", row.objectID, err.Error())
				}

				if (metaData.DestType == destination1.DestType || metaData.DestType == "") &&
					(metaData.DestID == destination1.DestID || metaData.DestID == "") {
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
		if dests, err := GetObjectDestinationsStatus(row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("Error in getObjectDestinationsStatus (objectID = %s). Error: %s", row.objectID, err.Error())
		} else if len(dests) != row.expectedDestNumber {
			t.Errorf("Wrong number of destinations: %d instead of %d (objectID = %s).", len(dests), row.expectedDestNumber, row.objectID)
		}
	}

	if err := deleteOrganization("myorg777"); err != nil {
		t.Errorf("deleteOrganization failed. Error: %s", err.Error())
	}
}

func TestESSObjectDeletedAPI(t *testing.T) {
	setupDB(common.Bolt)
	testESSObjectDeletedAPI(store, t)

	setupDB(common.InMemory)
	testESSObjectDeletedAPI(store, t)
}

func testESSObjectDeletedAPI(store storage.Storage, t *testing.T) {
	communications.Store = store
	common.InitObjectLocks()

	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer store.Stop()

	//common.Configuration.NodeType = common.ESS

	validObjects := []struct {
		orgID      string
		objectType string
		objectID   string
		metaData   common.MetaData
		data       []byte
	}{
		{"myorg777", "type2", "1",
			common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg777",
				DestinationPolicy: &common.Policy{
					Properties: []common.PolicyProperty{
						{Name: "j", Value: float64(42.0)},
						{Name: "k", Value: "ghjk"},
						{Name: "l", Value: float64(613)},
					},
					Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
					Services: []common.ServiceID{
						{OrgID: "myorg777", Arch: "amd64", ServiceName: "service1", Version: "1.0.0"},
					},
				},
			},
			[]byte("abc"),
		},
	}

	common.Configuration.NodeType = common.CSS

	for _, row := range validObjects {
		// Update object
		err := UpdateObject(row.orgID, row.objectType, row.objectID, row.metaData, row.data)
		if err != nil {
			t.Errorf("updateObject failed to update (objectID = %s). Error: %s", row.objectID, err.Error())
		}

		// ESS
		removedServices := []common.ServiceID{
			{OrgID: "myorg777", Arch: "amd64", ServiceName: "removed_service1", Version: "1.0.0"},
			{OrgID: "myorg777", Arch: "amd64", ServiceName: "removed_service2", Version: "1.0.0"},
			{OrgID: "myorg777", Arch: "amd64", ServiceName: "removed_service3", Version: "1.0.0"},
		}

		common.Configuration.NodeType = common.ESS

		serviceID := "myorg777/1.0.0/removed_service1"
		// before update removedDestinationPolicyServices, objectDeleted should fail
		if err := ObjectDeleted(serviceID, row.orgID, row.objectType, row.objectID); err == nil {
			t.Errorf("objectDeleted should fail if removedDestinationPolicyServices list is empty (objectID = %s)", row.objectID)
		}

		if err := store.UpdateRemovedDestinationPolicyServices(row.orgID, row.objectType, row.objectID, removedServices); err != nil {
			t.Errorf("Failed to store removedServices. Error: %s", err.Error())
		}

		if err := ObjectDeleted(serviceID, row.orgID, row.objectType, row.objectID); err != nil {
			t.Errorf("objectDeleted marked the sender's object as deleted  (objectID = %s)", row.objectID)
		}

		updatedRemovedServices, err := GetRemovedDestinationPolicyServicesFromESS(row.orgID, row.objectType, row.objectID)
		if err != nil {
			t.Errorf("Faild to GetRemovedDestinationPolicyServicesFromESS (objectID = %s)", row.objectID)
		}

		if len(updatedRemovedServices) != 2 {
			t.Errorf("Wrong updatedRemovedServices after objectDeleted")
		}

		for _, service := range updatedRemovedServices {
			if service.OrgID == "myorg777" && service.ServiceName == "removed_service1" && service.Version == "1.0.0" {
				t.Errorf("RemovedDestinationPolicyServices should not contain service: %s/%s/%s", service.OrgID, service.Version, service.ServiceName)
			}
		}
	}

}

func TestObjectDestinationsAPI(t *testing.T) {
	common.Configuration.NodeType = common.CSS
	setupDB(common.Mongo)
	testObjectDestinationsAPI(store, t)

	setupDB(common.Bolt)
	testObjectDestinationsAPI(store, t)
}

func testObjectDestinationsAPI(store storage.Storage, t *testing.T) {
	communications.Store = store
	common.InitObjectLocks()

	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer store.Stop()

	dests1 := []string{"device:dev1", "device2:dev", "device2:dev1"}
	dests2 := []string{"device3:dev1"}

	tests := []struct {
		metaData           common.MetaData
		expectedDestNumber int
		updateDests        bool
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg777",
			DestID: "dev1", DestType: "device", NoData: true}, 1, false},

		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg777",
			DestinationsList: dests1, NoData: true}, 3, false},

		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg777", NoData: true}, 4, false},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg777", NoData: false}, 4, false},
	}

	destination1 := common.Destination{DestOrgID: "myorg777", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination1); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination2 := common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination2); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination3 := common.Destination{DestOrgID: "myorg777", DestType: "device2", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination3); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	destination4 := common.Destination{DestOrgID: "myorg777", DestType: "device3", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(destination4); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	communications.Comm = &communications.TestComm{}
	if err := communications.Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	for _, test := range tests {
		// Update object
		err := UpdateObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID, test.metaData, nil)
		if err != nil {
			t.Errorf("UpdateObject failed to update (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
		}

		dests, err := GetObjectDestinationsStatus(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("GetObjectDestinationsStatus failed (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
		} else if len(dests) != test.expectedDestNumber {
			t.Errorf("GetObjectDestinationsStatus returned wrong number of destinations: %d instead of %d (objectID = %s)",
				len(dests), test.expectedDestNumber, test.metaData.ObjectID)
		}

		// Remove notifications for testing
		err = store.DeleteNotificationRecords(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID, "", "")
		if err != nil {
			t.Errorf("DeleteNotificationRecords failed (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
		}

		err = UpdateObjectDestinations(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID, dests2)
		if err != nil {
			t.Errorf("UpdateObjectDestinations failed (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
		} else {
			// There should be delete notifications for destination1
			notification, err := store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID, destination1.DestType, destination1.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					if test.metaData.NoData == true {
						t.Errorf("No delete notification record (objectID = %s)", test.metaData.ObjectID)
					}
				} else if test.metaData.NoData == false {
					t.Errorf("Notification record created for not ready to send object (objectID = %s)", test.metaData.ObjectID)
				} else {
					if notification.Status != common.Delete {
						t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, test.metaData.ObjectID)
					}
				}
			}
			if test.expectedDestNumber > 1 {
				// There should be delete notifications for destination2
				notification, err := store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
					destination2.DestType, destination2.DestID)
				if err != nil {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						if test.metaData.NoData == true {
							t.Errorf("No delete notification record (objectID = %s)", test.metaData.ObjectID)
						}
					} else if test.metaData.NoData == false {
						t.Errorf("Notification record created for not ready to send object (objectID = %s)", test.metaData.ObjectID)
					} else {
						if notification.Status != common.Delete {
							t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, test.metaData.ObjectID)
						}
					}
				}
				// There should be delete notifications for destination3
				notification, err = store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
					destination3.DestType, destination3.DestID)
				if err != nil {
					t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
				} else {
					if notification == nil {
						if test.metaData.NoData == true {
							t.Errorf("No delete notification record (objectID = %s)", test.metaData.ObjectID)
						}
					} else if test.metaData.NoData == false {
						t.Errorf("Notification record created for not ready to send object (objectID = %s)", test.metaData.ObjectID)
					} else {
						if notification.Status != common.Delete {
							t.Errorf("Wrong notification status: %s instead of delete (objectID = %s)", notification.Status, test.metaData.ObjectID)
						}
					}
				}
			}
			// Look for update notification for destination4
			notification, err = store.RetrieveNotificationRecord(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				destination4.DestType, destination4.DestID)
			if err != nil {
				t.Errorf("An error occurred in notification fetch (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
			} else {
				if notification == nil {
					if test.expectedDestNumber < 4 && test.metaData.NoData == true {
						t.Errorf("No notification record (objectID = %s)", test.metaData.ObjectID)
					}
				} else if test.metaData.NoData == false {
					t.Errorf("Notification record created for not ready to send object (objectID = %s)", test.metaData.ObjectID)
				} else {
					if test.expectedDestNumber == 4 && test.metaData.NoData == true {
						t.Errorf("Notification record created for already existing destination (objectID = %s)", test.metaData.ObjectID)
					} else if notification.Status != common.Update {
						t.Errorf("Wrong notification status: %s instead of update (objectID = %s)", notification.Status, test.metaData.ObjectID)
					}
				}
			}
		}
	}

}

func TestObjectWithPolicyAPI(t *testing.T) {
	common.Configuration.MongoDbName = "d_test_db"
	store = &storage.MongoStorage{}
	testObjectWithPolicyAPI(store, t)

	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
	boltStore := &storage.BoltStorage{}
	boltStore.Cleanup(true)
	store = boltStore
	testObjectWithPolicyAPI(store, t)
}

func testObjectWithPolicyAPI(store storage.Storage, t *testing.T) {
	tests := []struct {
		metaData common.MetaData
		recieved bool
		data     []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
			},
		}, true, []byte("0123456789abcdef")},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "d", Value: float64(98)},
					{Name: "e", Value: "asdf", Type: "string"},
					{Name: "f", Value: false},
				},
				Constraints: []string{"xyzzy=78", "vbnm=false"},
			},
		}, false, []byte("abcdefghijklmnopqrstuvwxyz")},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "g", Value: float64(-34)},
					{Name: "h", Value: "qwer"},
					{Name: "i", Value: float64(42), Type: "float"},
				},
				Constraints: []string{"x=15", "y=0.0"},
			},
		}, true, nil},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "j", Value: float64(42.0)},
					{Name: "k", Value: "ghjk"},
					{Name: "l", Value: float64(613)},
				},
				Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
				Services: []common.ServiceID{
					{OrgID: "myorg777", Arch: "amd64", ServiceName: "plony", Version: "1.0.0"},
				},
			},
		}, false, nil},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "myorg001",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "j", Value: float64(42.0)},
					{Name: "k", Value: "ghjk"},
					{Name: "l", Value: float64(613)},
				},
				Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
				Services: []common.ServiceID{
					{OrgID: "myorg777", Arch: "amd64", ServiceName: "plony", Version: "1.0.0"},
				},
			},
		}, false, nil},
	}

	destinations := []common.Destination{
		common.Destination{DestOrgID: "myorg000", DestType: "device", DestID: "dev",
			Communication: common.HTTPProtocol},
		common.Destination{DestOrgID: "myorg000", DestType: "device2", DestID: "dev",
			Communication: common.HTTPProtocol},
		common.Destination{DestOrgID: "myorg000", DestType: "device2", DestID: "dev1",
			Communication: common.HTTPProtocol},
	}

	communications.Store = store
	if err := store.Init(); err != nil {
		t.Errorf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}
	defer store.Stop()

	communications.Comm = &communications.TestComm{}
	if err := communications.Comm.StartCommunication(); err != nil {
		t.Errorf("Failed to start test communication. Error: %s", err.Error())
	}

	common.InitObjectLocks()

	for _, destination := range destinations {
		if err := store.StoreDestination(destination); err != nil {
			t.Errorf("Failed to store destination. Error: %s", err.Error())
		}
	}

	for _, test := range tests {
		// Delete the object first
		err := store.DeleteStoredObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			fmt.Printf("Error: %#v\n", err)
		}
		// Insert
		if err := UpdateObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
			test.metaData, test.data); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedMetaData, err := GetObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if storedMetaData.DestinationPolicy == nil {
				t.Errorf("DestinationPolicy nil in retrieved object (objectID = %s)\n", test.metaData.ObjectID)
			} else {
				equal := len(storedMetaData.DestinationPolicy.Properties) == len(test.metaData.DestinationPolicy.Properties) &&
					len(storedMetaData.DestinationPolicy.Constraints) == len(test.metaData.DestinationPolicy.Constraints)
				if equal {
					for index, property := range storedMetaData.DestinationPolicy.Properties {
						expectedProperty := test.metaData.DestinationPolicy.Properties[index]
						if expectedProperty.Name != property.Name || expectedProperty.Value != property.Value ||
							expectedProperty.Type != property.Type {
							equal = false
							break
						}
					}
				}
				if equal {
					for index, value := range storedMetaData.DestinationPolicy.Constraints {
						if value != test.metaData.DestinationPolicy.Constraints[index] {
							equal = false
							break
						}
					}
				}
				if !equal {
					t.Errorf("The retrieved DestinationPolicy %#v does not match the expected one %#v\n",
						storedMetaData.DestinationPolicy, test.metaData.DestinationPolicy)
				}

				destinations, err := store.GetObjectDestinations(test.metaData)
				if err != nil {
					t.Errorf("Failed to retrieve destinations for an object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				} else if len(destinations) != 0 {
					t.Errorf("Sent object with a policy to %d destinations.", len(destinations))
				}

				if test.data != nil {
					ok, err := PutObjectData(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID, bytes.NewReader(test.data))
					if !ok || err != nil {
						t.Errorf("Failed to update object's data (objectID = %s). Error: %s", test.metaData.ObjectID, err.Error())
					}

					destinations, err := store.GetObjectDestinations(test.metaData)
					if err != nil {
						t.Errorf("Failed to retrieve destinations for an object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
					} else if len(destinations) != 0 {
						t.Errorf("Sent object with a policy to %d destinations.", len(destinations))
					}
				}

				policyTimestamp := storedMetaData.DestinationPolicy.Timestamp

				if err := UpdateObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
					test.metaData, test.data); err != nil {
					t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				}
				storedMetaData, err := GetObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
				if err != nil {
					t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				} else if policyTimestamp >= storedMetaData.DestinationPolicy.Timestamp {
					t.Errorf("DestinationPolicy Timestamp wasn't incremented after update. Was %d, now is %d",
						policyTimestamp, storedMetaData.DestinationPolicy.Timestamp)
				}
			}
		}
	}

	objects, err := ListAllObjects("myorg000", "type1")
	if err != nil {
		t.Errorf("ListAllObjects failed to retrieve objects. Error: %s\n", err)
	} else {
		if len(objects) != len(tests)-1 {
			t.Errorf("Received %d objects. Expected %d\n", len(objects), len(tests))
		}
	}

	policyInfo, err := ListObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-1 {
		t.Errorf("Received %d objects with a destination policy. Expected %d\n", len(policyInfo), len(tests))
	}

	objectsMarkedReceived := 0
	for _, test := range tests {
		if test.recieved {
			objectsMarkedReceived++
			err = ObjectPolicyReceived(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
			if err != nil {
				t.Errorf("Failed to mark the destination policy of %s as received. Error: %s\n", test.metaData.ObjectID, err)
			}
		}
	}

	policyInfo, err = ListObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-1-objectsMarkedReceived {
		t.Errorf("Received %d objects with a destination policy. Expected %d. Total %d. Received %d\n",
			len(policyInfo), len(tests)-objectsMarkedReceived, len(tests)-1, objectsMarkedReceived)
	}

	policyInfo, err = ListObjectsWithDestinationPolicy("myorg000", true)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-1 {
		t.Errorf("Received %d objects with a destination policy. Expected %d\n", len(policyInfo), len(tests))
	}

	for _, test := range tests {
		if test.recieved {
			if err := UpdateObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
				test.metaData, test.data); err != nil {
				t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			}
			objectsMarkedReceived--
			break
		}
	}

	policyInfo, err = ListObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-1-objectsMarkedReceived {
		t.Errorf("Received %d objects with a destination policy. Expected %d. Total %d. Received %d\n",
			len(policyInfo), len(tests)-objectsMarkedReceived, len(tests), objectsMarkedReceived)
	}

	policyInfo, err = ListObjectsWithDestinationPolicyByService("myorg000", "myorg777", "plony")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != 1 {
		t.Errorf("Received %d objects with a destination policy. Expected %d.\n",
			len(policyInfo), 1)
	}
}
