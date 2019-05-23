package endtoend

import (
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
)

func TestStartup(t *testing.T) {
	testStartupHelper(common.Mongo, common.InMemory, false, t)

	testStartupHelper(common.Bolt, common.Bolt, false, t)
}

func TestOfflineEssRegister(t *testing.T) {
	testStartupHelper(common.Bolt, common.InMemory, true, t)
}

func testStartupHelper(cssStorageProvider string, essStorageProvider string, block bool, t *testing.T) {
	const numberOfESSs = 1

	cssInfo := startSyncService(true, cssStorageProvider, 1, t)

	var proxy *proxyInfo
	if block {
		proxy = startSyncServiceProxy()
		proxy.blockCalls(true)
	}

	essInfo := startSyncService(false, essStorageProvider, numberOfESSs, t)

	time.Sleep(6 * time.Second)

	cssClient := newClient(cssInfo[0], true)

	if block {
		time.Sleep(4 * time.Second)

		destinations, err := cssClient.GetDestinations()
		if err != nil {
			t.Errorf("Failed to get destinations from %s%d. Error: %s", cssInfo[0].nodeType, cssInfo[0].id, err)
		} else {
			if 0 != len(destinations) {
				t.Errorf("Received %d destinations instead of the expected 0", len(destinations))
			}
		}

		proxy.blockCalls(false)
		time.Sleep(4 * time.Second)
	}

	destinations, err := cssClient.GetDestinations()
	if err != nil {
		t.Errorf("Failed to get destinations from %s%d. Error: %s", cssInfo[0].nodeType, cssInfo[0].id, err)
	} else {
		if numberOfESSs != len(destinations) {
			t.Errorf("Received %d destinations instead of the expected %d", len(destinations), numberOfESSs)
		}
	}

	stopSyncService(cssInfo, t)
	stopSyncService(essInfo, t)
}
