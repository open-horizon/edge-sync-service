package storage

import (
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestMongoStorageObjects(t *testing.T) {
	testStorageObjects(common.Mongo, t)
}

func TestMongoStorageObjectsWithPolicy(t *testing.T) {
	testStorageObjectsWithPolicy(common.Mongo, t)
}

func TestMongoStorageGetObjectWithFilters(t *testing.T) {
	testGetObjectWithFilters(common.Mongo, t)
}

func TestMongoStorageObjectActivation(t *testing.T) {
	testStorageObjectActivation(common.Mongo, t)
}

func TestMongoStorageObjectExpiration(t *testing.T) {
	testStorageObjectExpiration(common.Mongo, t)
}

func TestMongoStorageObjectData(t *testing.T) {
	testStorageObjectData(common.Mongo, t)
}

func TestMongoStorageOrgDeleteObjects(t *testing.T) {
	testStorageOrgDeleteObjects(common.Mongo, t)
}

func TestMongoStorageDestinations(t *testing.T) {
	common.Configuration.MongoDbName = "d_test_db"
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
	testStorageNotifications(common.Mongo, t)
}

func TestMongoStorageOrgDeleteNotifications(t *testing.T) {
	testStorageOrgDeleteNotifications(common.Mongo, t)
}

func TestMongoStorageOrgDeleteACLs(t *testing.T) {
	testStorageOrgDeleteACLs(common.Mongo, t)
}

func TestMongoStorageMessagingGroups(t *testing.T) {
	testStorageMessagingGroups(common.Mongo, t)
}

func TestMongoStorageObjectDestinations(t *testing.T) {
	testStorageObjectDestinations(common.Mongo, t)
}

func TestMongoStorageWebhooks(t *testing.T) {
	testStorageWebhooks(common.Mongo, t)
}

func TestMongoStorageOrganizations(t *testing.T) {
	testStorageOrganizations(common.Mongo, t)
}

func TestMongoStorageInactiveDestinations(t *testing.T) {
	testStorageInactiveDestinations(common.Mongo, t)
}
