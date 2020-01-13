package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
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
		if deletedDests, err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(deletedDests) != 0 {
				t.Errorf("StoreObject for new object returned deleted destinations (objectID = %s)\n", test.metaData.ObjectID)
			}
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
		time.Sleep(20 * time.Millisecond)
		if _, err := store.StoreObject(test.metaData, nil, test.status); err != nil {
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
	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 0 {
		t.Errorf("RetrieveObjects returned objects\n")
	}
}

func testStorageObjectsWithPolicy(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	tests := []struct {
		metaData common.MetaData
		recieved bool
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
		}, true},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "d", Value: float64(98)},
					{Name: "e", Value: "asdf", Type: "string"},
					{Name: "f", Value: false},
				},
				Constraints: []string{"xyzzy=78", "vbnm=false"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "xyzzy", Version: "1.0.0"},
				},
			},
		}, false},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "g", Value: float64(-34)},
					{Name: "h", Value: "qwer"},
					{Name: "i", Value: float64(42), Type: "float"},
				},
				Constraints: []string{"x=15", "y=0.0"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "xyzzy", Version: "1.0.0"},
				},
			},
		}, true},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg000",
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "j", Value: float64(42.0)},
					{Name: "k", Value: "ghjk"},
					{Name: "l", Value: float64(613)},
				},
				Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "wompus", Version: "1.0.0"},
				},
			},
		}, false},
	}

	testServiceUpdate := common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000",
		DestinationPolicy: &common.Policy{
			Properties: []common.PolicyProperty{
				{Name: "d", Value: float64(98)},
				{Name: "e", Value: "asdf", Type: "string"},
				{Name: "f", Value: false},
			},
			Constraints: []string{"xyzzy=78", "vbnm=false"},
			Services: []common.ServiceID{
				{OrgID: "plover", Arch: "amd64", ServiceName: "testServiceUpdate", Version: "1.0.0"},
			},
		},
	}

	for _, test := range tests {
		// Delete the object first
		if err := store.DeleteStoredObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		// Insert
		if _, err := store.StoreObject(test.metaData, nil, common.NotReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
		storedMetaData, err := store.RetrieveObject(test.metaData.DestOrgID,
			test.metaData.ObjectType, test.metaData.ObjectID)
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

				policyTimestamp := storedMetaData.DestinationPolicy.Timestamp

				if _, err := store.StoreObject(test.metaData, nil, common.NotReadyToSend); err != nil {
					t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				}
				storedMetaData, err := store.RetrieveObject(test.metaData.DestOrgID,
					test.metaData.ObjectType, test.metaData.ObjectID)
				if err != nil {
					t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
				} else if policyTimestamp >= storedMetaData.DestinationPolicy.Timestamp {
					t.Errorf("DestinationPolicy Timestamp wasn't incremented after update. Was %d, now is %d",
						policyTimestamp, storedMetaData.DestinationPolicy.Timestamp)
				}
			}
		}
	}

	policyInfo, err := store.RetrieveObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests) {
		t.Errorf("Received %d objects with a destination policy. Expected %d\n", len(policyInfo), len(tests))
	}

	objectsMarkedReceived := 0
	for _, test := range tests {
		if test.recieved {
			objectsMarkedReceived++
			err = store.MarkDestinationPolicyReceived(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID)
			if err != nil {
				t.Errorf("Failed to mark the destination policy of %s as received. Error: %s\n", test.metaData.ObjectID, err)
			}
		}
	}

	policyInfo, err = store.RetrieveObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-objectsMarkedReceived {
		t.Errorf("Received %d objects with a destination policy. Expected %d. Total %d. Received %d\n",
			len(policyInfo), len(tests)-objectsMarkedReceived, len(tests), objectsMarkedReceived)
	}

	policyInfo, err = store.RetrieveObjectsWithDestinationPolicy("myorg000", true)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests) {
		t.Errorf("Received %d objects with a destination policy. Expected %d\n", len(policyInfo), len(tests))
	}

	for _, test := range tests {
		if test.recieved {
			if _, err := store.StoreObject(test.metaData, nil, common.NotReadyToSend); err != nil {
				t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			}
			objectsMarkedReceived--
			break
		}
	}

	policyInfo, err = store.RetrieveObjectsWithDestinationPolicy("myorg000", false)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != len(tests)-objectsMarkedReceived {
		t.Errorf("Received %d objects with a destination policy. Expected %d. Total %d. Received %d\n",
			len(policyInfo), len(tests)-objectsMarkedReceived, len(tests), objectsMarkedReceived)
	}

	policyInfo, err = store.RetrieveObjectsWithDestinationPolicyByService("myorg000", "plover", "xyzzy")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(policyInfo) != 2 {
		t.Errorf("Received %d objects with a destination policy. Expected %d.\n",
			len(policyInfo), 2)
	}

	// test destination policy service update
	if _, err := store.StoreObject(testServiceUpdate, nil, common.NotReadyToSend); err != nil {
		t.Errorf("Failed to store object (objectID = %s). Error: %s\n", testServiceUpdate.ObjectID, err.Error())
	}
	storedMetaData, err := store.RetrieveObject(testServiceUpdate.DestOrgID,
		testServiceUpdate.ObjectType, testServiceUpdate.ObjectID)
	if err != nil {
		t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", testServiceUpdate.ObjectID, err.Error())
	} else {
		if storedMetaData.DestinationPolicy == nil {
			t.Errorf("DestinationPolicy nil in retrieved object (objectID = %s)\n", testServiceUpdate.ObjectID)
		} else {
			policyServices := storedMetaData.DestinationPolicy.Services
			if len(policyServices) != len(testServiceUpdate.DestinationPolicy.Services) {
				t.Errorf("The retrieved DestinationPolicy service %#v does not match the expected one %#v\n",
					policyServices, testServiceUpdate.DestinationPolicy.Services)
			} else {
				if policyServices[0].ServiceName != testServiceUpdate.DestinationPolicy.Services[0].ServiceName {
					t.Errorf("The retrieved DestinationPolicy service name %#v does not match the expected one %#v\n",
						policyServices[0].ServiceName, testServiceUpdate.DestinationPolicy.Services[0].ServiceName)
				}

				lastDestinationPolicyServices := storedMetaData.LastDestinationPolicyServices
				oldDestinationPolicyServices := tests[1].metaData.DestinationPolicy.Services
				if len(lastDestinationPolicyServices) != len(oldDestinationPolicyServices) {
					t.Errorf("The LastDestinationPolicyServices %#v does not match the removed destination policy services %#v\n",
						lastDestinationPolicyServices, oldDestinationPolicyServices)
				} else {
					if lastDestinationPolicyServices[0].ServiceName != oldDestinationPolicyServices[0].ServiceName {
						t.Errorf("The LastDestinationPolicyService name %#v does not match the removed destination policy service %#v\n",
							lastDestinationPolicyServices[0].ServiceName, oldDestinationPolicyServices[0].ServiceName)
					}
				}
			}
		}
	}

}

func testGetObjectWithFilters(storageType string, t *testing.T) {
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	dest1 := common.Destination{DestOrgID: "myorg111", DestType: "myDestType5", DestID: "myDestID5c", Communication: common.HTTPProtocol}
	destArray := []string{"myDestType5:myDestID5c"}

	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	tests := []struct {
		metaData common.MetaData
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg111", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
			},
			Expiration: "",
		}},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg111", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "d", Value: float64(98)},
					{Name: "e", Value: "asdf", Type: "string"},
					{Name: "f", Value: false},
				},
				Constraints: []string{"xyzzy=78", "vbnm=false"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "xyzzy", Version: "1.0.0"},
				},
			},
			Expiration: "2012-08-14T14:00:00Z",
		}},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg111", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "g", Value: float64(-34)},
					{Name: "h", Value: "qwer"},
					{Name: "i", Value: float64(42), Type: "float"},
				},
				Constraints: []string{"x=15", "y=0.0"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "xyzzy", Version: "1.0.0"},
				},
			},
			Expiration: "2013-08-14T14:00:00Z",
		}},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorg111", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "j", Value: float64(42.0)},
					{Name: "k", Value: "ghjk"},
					{Name: "l", Value: float64(613)},
				},
				Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "wompus", Version: "1.0.0"},
				},
			},
			Expiration: "2014-08-14T14:00:00Z",
		}},
		{common.MetaData{ObjectID: "5a", ObjectType: "type1", DestOrgID: "myorg111", DestType: "myDestType5", DestID: "myDestID5a", NoData: true, Expiration: ""}},
		{common.MetaData{ObjectID: "5b", ObjectType: "type1", DestOrgID: "myorg111", DestType: "myDestType5", DestID: "myDestID5b", NoData: false, Expiration: ""}},
		{common.MetaData{ObjectID: "5c", ObjectType: "type1", DestOrgID: "myorg111", DestinationsList: destArray, NoData: false, Expiration: ""}},
		{common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg111", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "j", Value: float64(42.0)},
					{Name: "k", Value: "ghjk"},
					{Name: "l", Value: float64(613)},
				},
				Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "wompus", Version: "1.0.0"},
				},
			},
			Expiration: "2014-08-14T14:00:00Z",
		}},
		{common.MetaData{ObjectID: "5a", ObjectType: "type2", DestOrgID: "myorg111", DestType: "myDestType5", DestID: "myDestID5a", NoData: true, Expiration: ""}},
		{common.MetaData{ObjectID: "5b", ObjectType: "type2", DestOrgID: "myorg111", DestType: "myDestType5", DestID: "myDestID5b", NoData: false, Expiration: ""}},
		{common.MetaData{ObjectID: "5c", ObjectType: "type2", DestOrgID: "myorg111", DestinationsList: destArray, NoData: false, Expiration: ""}},
	}

	for _, test := range tests {
		// delete
		if err := store.DeleteStoredObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}

		// insert
		if _, err := store.StoreObject(test.metaData, nil, common.ReadyToSend); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}

		// retrieve
		if _, err := store.RetrieveObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	objects, err := store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with filters. Error: %s\n", err)
	}
	if len(objects) != len(tests) {
		t.Errorf("Retrieved %d objects with filters. Expected %d\n", len(objects), len(tests))
	}

	destinationPolicy := true
	expectedResultCount := 5
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "", "", "", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with filters. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 2
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "plover", "xyzzy", "", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given serviceOrgId and servicename in destination policy. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 1
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "plover", "xyzzy", "d", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given serviceOrgId, serviceName and propertyName in destination policy. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 1
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "", "", "d", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given propertyName in destination policy. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 1
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "", "", "", 0, "type2", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given propertyName in destination policy. Expected %d\n", len(objects), expectedResultCount)
	}

	destinationPolicy = false
	expectedResultCount = 6
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "", "", "", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects without destination policy. Expected %d\n", len(objects), expectedResultCount)
	}

	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "d", 0, "", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != len(tests) {
		t.Errorf("Retrieved %d objects should not check destination policy subfield if desitinationPolicy is nil. Expected %d\n", len(objects), len(tests))
	}

	expectedResultCount = 6
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "myDestType5", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 2
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "myDestType5", "myDestID5a", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destinationType and destinationID. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 2
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "myDestType5", "myDestID5c", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destinationType and destinationID. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 4
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "type2", "", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 1
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "type2", "5a", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
	}

	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "5a", "", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != len(tests) {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 3
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "type2", "", "myDestType5", "", nil, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 7
	noData := true
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "", "", &noData, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with noData set to true. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 2
	destinationPolicy = false
	objects, err = store.RetrieveObjectsWithFilters("myorg111", &destinationPolicy, "", "", "", 0, "", "", "", "", &noData, "")
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects without destination policy and noData set to false. Expected %d\n", len(objects), expectedResultCount)
	}

	expectedResultCount = 2
	expirationTimeBefore := "2013-08-15T14:00:00Z"
	objects, err = store.RetrieveObjectsWithFilters("myorg111", nil, "", "", "", 0, "", "", "", "", nil, expirationTimeBefore)
	if err != nil {
		t.Errorf("Failed to retrieve the objects with a destination policy. Error: %s\n", err)
	}
	if len(objects) != expectedResultCount {
		t.Errorf("Retrieved %d objects with given destination type. Expected %d\n", len(objects), expectedResultCount)
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

	activationTime1 := time.Now().Add(time.Second * 2).UTC().Format(time.RFC3339)
	activationTime2 := time.Now().Add(time.Second * 6).UTC().Format(time.RFC3339)

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
		if _, err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	objectsToActivate, err := store.GetObjectsToActivate()
	if err != nil {
		t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
	}
	if len(objectsToActivate) != 0 {
		t.Errorf("GetObjectsToActivate returned objects.\n")
	}

	time.Sleep(4 * time.Second)

	objectsToActivate, err = store.GetObjectsToActivate()
	if err != nil {
		t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
	} else {
		if len(objectsToActivate) != 1 {
			t.Errorf("GetObjectsToActivate returned incorrect number of objects: %d instead of 1.\n", len(objectsToActivate))
			for index, object := range objectsToActivate {
				t.Errorf("   GetObjectsToActivate object #%d:    %s:%s", index, object.ObjectType, object.ObjectID)
			}
		} else if objectsToActivate[0].ObjectID != "2" {
			t.Errorf("GetObjectsToActivate returned incorrect objects: id=%s instead of 2.\n", objectsToActivate[0].ObjectID)
		}
	}

	time.Sleep(5 * time.Second)

	objectsToActivate, err = store.GetObjectsToActivate()
	if err != nil {
		t.Errorf("GetObjectsToActivate failed. Error: %s\n", err.Error())
	} else {
		if len(objectsToActivate) != 2 {
			t.Errorf("GetObjectsToActivate returned incorrect number of objects: %d instead of 2.\n", len(objectsToActivate))
		} else if (objectsToActivate[0].ObjectID != "2" && objectsToActivate[0].ObjectID != "3") ||
			(objectsToActivate[1].ObjectID != "2" && objectsToActivate[1].ObjectID != "3") {
			t.Errorf("GetObjectsToActivate returned incorrect objects.\n")
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

	dest1 := common.Destination{DestOrgID: "org555", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	tests := []struct {
		metaData common.MetaData
		status   string
		data     []byte
		newData  []byte
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "org555", DestID: "dev1", DestType: "device"},
			common.ReadyToSend, []byte("abcdefghijklmnopqrstuvwxyz"), []byte("new")},
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "org555", DestID: "dev1", DestType: "device", MetaOnly: true},
			common.ReadyToSend, nil, nil},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "org555", DestID: "dev1", DestType: "device",
			Inactive: true}, common.CompletelyReceived, []byte("abcdefghijklmnopqrstuvwxyz"), []byte("new")},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "org555", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend, nil, []byte("new")},
	}

	for i, test := range tests {
		test.metaData.ObjectSize = int64(len(test.data) + len(test.newData))
		// Insert
		if _, err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}

		if test.metaData.MetaOnly {
			// Instance id should be greater than the data id, because the instance id should be set and data id not
			storedMetaData, err := store.RetrieveObject(test.metaData.DestOrgID,
				test.metaData.ObjectType, test.metaData.ObjectID)
			if err != nil {
				t.Errorf("Failed to retrieve object(objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
			} else {
				if storedMetaData.InstanceID <= storedMetaData.DataID {
					t.Errorf("InstanceID <= DataID (objectID = %s)", test.metaData.ObjectID)
				}
			}
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
			testData := test.data
			if test.metaData.MetaOnly {
				testData = append(tests[i-1].data, tests[i-1].newData...) // The data doesn't change in the meta only update
			}
			if n != len(testData) {
				t.Errorf("Incorrect data size 's data from the returned reader (objectID = %s): %d instead of %d",
					test.metaData.ObjectID, n, len(testData))
			}
			data = data[:n]
			if string(data) != string(testData) {
				t.Errorf("Incorrect data (objectID = %s): %s instead of %s",
					test.metaData.ObjectID, string(data), string(testData))
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

	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
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
	} else if len(notifications) != 1 && storageType == common.Mongo {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 1\n", len(notifications))
	} else if len(notifications) != 0 && storageType == common.InMemory {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 0\n", len(notifications))
	}

	if notifications, err := store.RetrievePendingNotifications(tests[5].n.DestOrgID, "", ""); err != nil {
		t.Errorf("RetrievePendingNotifications failed. Error: %s\n", err.Error())
	} else if len(notifications) != 2 && storageType == common.Mongo {
		t.Errorf("RetrievePendingNotifications returned wrong number of notifications: %d instead of 2\n", len(notifications))
	} else if len(notifications) != 0 && storageType == common.InMemory {
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

func testStorageObjectExpiration(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	defer store.Stop()

	dest1 := common.Destination{DestOrgID: "1myorg1", DestType: "device", DestID: "dev1", Communication: common.MQTTProtocol}
	if err := store.StoreDestination(dest1); err != nil {
		t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
	}

	expirationTime1 := time.Now().Add(time.Second * 1).UTC().Format(time.RFC3339)
	expirationTime2 := time.Now().Add(time.Second * 6).UTC().Format(time.RFC3339)

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
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Expiration: expirationTime2}, common.CompletelyReceived},
	}

	notifications := []struct {
		n               common.Notification
		shouldBeRemoved bool
	}{
		{common.Notification{ObjectID: "1", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Status: common.Update, InstanceID: 5}, false},
		{common.Notification{ObjectID: "2", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev12", DestType: "device",
			Status: common.Update, InstanceID: 5}, true},
		{common.Notification{ObjectID: "2", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev12", DestType: "device",
			Status: common.Update, InstanceID: 5}, true},
		{common.Notification{ObjectID: "3", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Status: common.Getdata}, true},
		{common.Notification{ObjectID: "4", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Status: common.Data}, false},
		{common.Notification{ObjectID: "5", ObjectType: "type1", DestOrgID: "1myorg1", DestID: "dev1", DestType: "device",
			Status: common.AckConsumed, InstanceID: 9}, false},
	}

	for _, test := range tests {
		// Insert
		if _, err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}

	for _, n := range notifications {
		if err := store.UpdateNotificationRecord(n.n); err != nil {
			t.Errorf("UpdateNotificationRecord failed. Error: %s\n", err.Error())
		}
	}

	objects, err := store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 3 {
		t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 3\n", len(objects))
	}

	time.Sleep(3 * time.Second)

	store.PerformMaintenance()

	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 2 {
		t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 2\n", len(objects))
	}

	time.Sleep(6 * time.Second)

	store.PerformMaintenance()

	objects, err = store.RetrieveObjects(tests[0].metaData.DestOrgID, tests[0].metaData.DestType, tests[0].metaData.DestID, common.ResendAll)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if len(objects) != 1 {
		t.Errorf("RetrieveObjects returned incorrect number of objects: %d instead of 1\n", len(objects))
	} else if objects[0].ObjectID != "1" {
		t.Errorf("Incorrect object: %s instead of objectId=1\n", objects[0].ObjectID)
	}

	object, err := store.RetrieveObject(tests[3].metaData.DestOrgID, tests[3].metaData.ObjectType, tests[3].metaData.ObjectID)
	if err != nil {
		t.Errorf("RetrieveObjects failed. Error: %s\n", err.Error())
	} else if object == nil {
		t.Errorf("No object objectId=4\n")
	}

	for _, n := range notifications {
		if storedNotification, err := store.RetrieveNotificationRecord(n.n.DestOrgID, n.n.ObjectType, n.n.ObjectID, n.n.DestType,
			n.n.DestID); (err == nil && storedNotification != nil) && n.shouldBeRemoved {
			t.Errorf("Notification wasn't removed (object ID = %s)\n", n.n.ObjectID)
		}
	}
}

func testStorageOrgDeleteObjects(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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
		if _, err := store.StoreObject(test.metaData, test.data, test.status); err != nil {
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

func testStorageOrgDeleteNotifications(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
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

func testStorageMessagingGroups(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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

func testStorageObjectDestinations(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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
		metaData             common.MetaData
		status               string
		numberOfDeletedDests int
		numberOfAddedDests   int
		deletedDest          *common.Destination
		addedDest            *common.Destination
	}{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device"},
			common.NotReadyToSend, 0, 0, nil, nil},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "org444", DestType: "device",
			Inactive: true}, common.NotReadyToSend, 0, 0, nil, nil},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device",
			Inactive: true}, common.NotReadyToSend, 0, 0, nil, nil},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device",
			NoData: true}, common.ReadyToSend, 0, 0, nil, nil},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "org444",
			DestinationsList: destArray, NoData: true}, common.ReadyToSend, 0, 0, nil, nil},
		{common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "org444", DestinationsList: destArray,
			NoData: true}, common.ReadyToSend, 0, 1, nil, &dest2},
		{common.MetaData{ObjectID: "5", ObjectType: "type1", DestOrgID: "org444", DestID: "dev1", DestType: "device",
			NoData: true}, common.ReadyToSend, 1, 0, &dest2, nil},
	}

	for _, test := range tests {
		// Delete the object first
		if err := store.DeleteStoredObject(test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		}
	}
	for _, test := range tests {
		// Insert
		if deletedDests, err := store.StoreObject(test.metaData, nil, test.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", test.metaData.ObjectID, err.Error())
		} else {
			if len(deletedDests) != test.numberOfDeletedDests {
				t.Errorf("StoreObject returned wrong number of deleted destinations: %d instead of %d (objectID = %s).\n",
					len(deletedDests), test.numberOfDeletedDests, test.metaData.ObjectID)
			} else if len(deletedDests) == 1 && deletedDests[0].Destination != *test.deletedDest {
				t.Errorf("StoreObject returned wrong deleted destination (objectID = %s).\n", test.metaData.ObjectID)
			}
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
		if _, err := store.UpdateObjectDeliveryStatus(common.Delivered, "", test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
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
		if _, err := store.UpdateObjectDeliveryStatus(common.Error, "Error", test.metaData.DestOrgID, test.metaData.ObjectType, test.metaData.ObjectID,
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

func testStorageOrganizations(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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

func testStorageInactiveDestinations(storageType string, t *testing.T) {
	common.Configuration.NodeType = common.CSS
	store, err := setUpStorage(storageType)
	if err != nil {
		t.Errorf(err.Error())
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
			n.n.DestID); (err == nil && storedNotification != nil) && n.shouldBeRemoved {
			t.Errorf("Notification wasn't removed (object ID = %s)\n", n.n.ObjectID)
		}
	}

}

func setUpStorage(storageType string) (Storage, error) {
	var store Storage
	switch storageType {
	case common.InMemory:
		store = &Cache{Store: &InMemoryStorage{}}
	case common.Bolt:
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		path := common.Configuration.PersistenceRootPath + "/sync/db/"
		os.RemoveAll(path)
		boltStore := &BoltStorage{}
		store = &Cache{Store: boltStore}
	case common.Mongo:
		common.Configuration.MongoDbName = "d_test_db"
		store = &Cache{Store: &MongoStorage{}}
	}
	if err := store.Init(); err != nil {
		return nil, &Error{fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())}
	}
	return store, nil
}
