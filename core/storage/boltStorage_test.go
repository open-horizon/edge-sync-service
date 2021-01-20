package storage

import (
	"os"
	"testing"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestBoltStorageStorageObjects(t *testing.T) {
	testStorageObjects(common.Bolt, t)
}

func TestBoltStorageObjectsWithPolicy(t *testing.T) {
	testStorageObjectsWithPolicy(common.Bolt, t)
}

func TestBoltStorageGetObjectWithFilters(t *testing.T) {
	testGetObjectWithFilters(common.Bolt, t)
}

func TestBoltStorageStorageObjectActivation(t *testing.T) {
	testStorageObjectActivation(common.Bolt, t)
}

func TestBoltStorageObjectData(t *testing.T) {
	testStorageObjectData(common.Bolt, t)
}

func TestBoltStorageNotifications(t *testing.T) {
	testStorageNotifications(common.Bolt, t)
}

func TestBoltStorageDestinations(t *testing.T) {
	store := &BoltStorage{}
	store.Cleanup(true)
	common.Configuration.NodeType = common.ESS
	dir, _ := os.Getwd()
	common.Configuration.PersistenceRootPath = dir + "/persist"
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
		} else if protocol != common.Configuration.CommunicationProtocol {
			t.Errorf("RetrieveDestinationProtocol returned incorrect protocol %s instead of %s\n", protocol, common.Configuration.CommunicationProtocol)
		}

		if dest, err := store.RetrieveDestination(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); err != nil {
			t.Errorf("RetrieveDestination failed. Error: %s\n", err.Error())
		} else {
			if dest.Communication != common.Configuration.CommunicationProtocol {
				t.Errorf("Incorrect protocol %s instead of %s\n", dest.Communication, common.Configuration.CommunicationProtocol)
			} else if dest.DestOrgID != test.dest.DestOrgID {
				t.Errorf("Incorrect org %s instead of %s\n", dest.DestOrgID, test.dest.DestOrgID)
			} else if dest.DestType != test.dest.DestType {
				t.Errorf("Incorrect type %s instead of %s\n", dest.DestType, test.dest.DestType)
			} else if dest.DestID != test.dest.DestID {
				t.Errorf("Incorrect ID %s instead of %s\n", dest.DestID, test.dest.DestID)
			}
		}
	}

	if dests, err := store.RetrieveDestinations("myorg123", "device"); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 0 {
		t.Errorf("Wrong number of destinations: %d instead of 0\n", len(dests))
	}

	if dests, err := store.RetrieveDestinations("myorg123", "device2"); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 0 {
		t.Errorf("Wrong number of destinations: %d instead of 0\n", len(dests))
	}

	for _, test := range tests {
		if err := store.DeleteDestination(test.dest.DestOrgID, test.dest.DestType, test.dest.DestID); err != nil {
			t.Errorf("DeleteDestination failed. Error: %s\n", err.Error())
		}
	}

	// CSS
	store.Cleanup(true)
	common.Configuration.NodeType = common.CSS

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
			t.Errorf("RetrieveDestination failed. Error: %s\n", err.Error())
		} else {
			if dest.Communication != test.dest.Communication {
				t.Errorf("Incorrect protocol %s instead of %s\n", dest.Communication, test.dest.Communication)
			} else if dest.DestOrgID != test.dest.DestOrgID {
				t.Errorf("Incorrect org %s instead of %s\n", dest.DestOrgID, test.dest.DestOrgID)
			} else if dest.DestType != test.dest.DestType {
				t.Errorf("Incorrect type %s instead of %s\n", dest.DestType, test.dest.DestType)
			} else if dest.DestID != test.dest.DestID {
				t.Errorf("Incorrect ID %s instead of %s\n", dest.DestID, test.dest.DestID)
			}
		}
	}

	if dests, err := store.RetrieveDestinations("myorg123", "device"); err != nil {
		t.Errorf("RetrieveDestinations failed. Error: %s\n", err.Error())
	} else if len(dests) != 1 {
		t.Errorf("Wrong number of destinations: %d instead of 1\n", len(dests))
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
	}

}

func TestBoltCacheStorageDestinations(t *testing.T) {
	boltStore := &BoltStorage{}
	boltStore.Cleanup(true)
	store := &Cache{Store: boltStore}
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
			t.Errorf("RetrieveDestination failed. Error: %s\n", err.Error())
		} else if dest.Communication != test.dest.Communication {
			t.Errorf("Incorrect protocol %s instead of %s\n", dest.Communication, test.dest.Communication)
		} else if dest.DestOrgID != test.dest.DestOrgID {
			t.Errorf("Incorrect org %s instead of %s\n", dest.DestOrgID, test.dest.DestOrgID)
		} else if dest.DestType != test.dest.DestType {
			t.Errorf("Incorrect type %s instead of %s\n", dest.DestType, test.dest.DestType)
		} else if dest.DestID != test.dest.DestID {
			t.Errorf("Incorrect ID %s instead of %s\n", dest.DestID, test.dest.DestID)
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
}

func TestBoltStorageWebhooks(t *testing.T) {
	testStorageWebhooks(common.Bolt, t)
}

func TestBoltStorageObjectExpiration(t *testing.T) {
	testStorageObjectExpiration(common.Bolt, t)
}

func TestBoltStorageOrgDeleteObjects(t *testing.T) {
	testStorageOrgDeleteObjects(common.Bolt, t)
}

func TestBoltStorageOrgDeleteNotifications(t *testing.T) {
	testStorageOrgDeleteNotifications(common.Bolt, t)
}

func TestBoltStorageOrgDeleteACLs(t *testing.T) {
	testStorageOrgDeleteACLs(common.Bolt, t)
}

func TestBoltStorageMessagingGroups(t *testing.T) {
	testStorageMessagingGroups(common.Bolt, t)
}

func TestBoltStorageObjectDestinations(t *testing.T) {
	testStorageObjectDestinations(common.Bolt, t)
}

func TestBoltStorageOrganizations(t *testing.T) {
	testStorageOrganizations(common.Bolt, t)
}

func TestBoltStorageInactiveDestinations(t *testing.T) {
	testStorageInactiveDestinations(common.Bolt, t)
}
