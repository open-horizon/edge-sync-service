package storage

import (
	"github.com/open-horizon/edge-sync-service/common"
	"testing"
)

func TestCouchStorageObject(t *testing.T) {
	testStorageObjects(common.Couch, t)
}

func TestCouchObjectActivation(t *testing.T) {
	testStorageObjectActivation(common.Couch, t)
}

func TestCouchObjectDestinations(t *testing.T) {
	testStorageObjectDestinations(common.Couch, t)
}
