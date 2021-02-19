package base

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

func TestHandleDestinations(t *testing.T) {
	testHandleDestinations(common.Bolt, t)
	testHandleDestinations(common.Mongo, t)
}

func testHandleDestinations(storageType string, t *testing.T) {
	if status := testAPIServerSetup(common.CSS, storageType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()
	defer security.Stop()

	destinations := []common.Destination{
		common.Destination{DestOrgID: "myorg5", DestType: "my-type", DestID: "my01", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "myorg5", DestType: "my-type", DestID: "my02", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "myorg5", DestType: "my-type2", DestID: "my01", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "plover5", DestType: "plover-type", DestID: "plover01", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "plover5", DestType: "plover-type", DestID: "plover02", Communication: common.MQTTProtocol},
	}

	for _, destination := range destinations {
		if err := store.StoreDestination(destination); err != nil {
			t.Errorf("Failed to store detination %#v. Error: %s\n", destination, err)
		}
	}

	testData := []struct {
		appKey        string
		orgID         string
		status        int
		expectedCount int
	}{
		{"testerUser@myorg5", "myorg5", http.StatusOK, 3},
		{"testerAdmin@plover5", "plover5", http.StatusOK, 2},
		{"tester@plover5", "plover5", http.StatusForbidden, -1}}

	for _, test := range testData {
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(http.MethodGet, test.orgID, nil)
		request.SetBasicAuth(test.appKey, "")

		handleDestinations(writer, request)
		if writer.statusCode != test.status {
			t.Errorf("The call to handleDestinations returned %d instead of %d\n", writer.statusCode, test.status)
		} else if test.status != http.StatusForbidden {
			decoder := json.NewDecoder(&writer.body)
			var data []common.Destination
			if err := decoder.Decode(&data); err != nil {
				t.Errorf("Failed to unmarshall destinations. Error: %s\n", err)
			} else {
				if len(data) != test.expectedCount {
					t.Errorf("For the org %s we were supposed to fetch %d destinations. Fetched %d destinations\n", test.orgID, test.expectedCount,
						len(data))
				}
			}
		}
	}
}

func TestHandleDestinationsInvalidCalls(t *testing.T) {
	testHandleDestinationsInvalidCalls(common.Mongo, t)
	testHandleDestinationsInvalidCalls(common.Bolt, t)
}

func testHandleDestinationsInvalidCalls(storageType string, t *testing.T) {
	if status := testAPIServerSetup(common.CSS, storageType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()
	defer security.Stop()

	testData := []struct {
		method             string
		theURL             string
		expectedHTTPStatus int
	}{
		{http.MethodPost, "myorg1", http.StatusMethodNotAllowed},
		{http.MethodGet, "", http.StatusBadRequest},
		{http.MethodGet, "plover1", http.StatusNotFound},
	}

	for _, test := range testData {
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(test.method, test.theURL, nil)
		request.SetBasicAuth("testerUser@"+test.theURL, "")

		handleDestinations(writer, request)
		if writer.statusCode != test.expectedHTTPStatus {
			t.Errorf("handleDestinations of %s returned a status of %d instead of %d\n", test.theURL, writer.statusCode, test.expectedHTTPStatus)
		}
	}
}

func TestMisceleneousHandlers(t *testing.T) {
	testMisceleneousHandlers(common.Mongo, t)
	testMisceleneousHandlers(common.Bolt, t)
}

func testMisceleneousHandlers(storageType string, t *testing.T) {
	if status := testAPIServerSetup(common.ESS, storageType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	// resend
	testResend := []struct {
		method             string
		expectedHTTPStatus int
	}{{http.MethodGet, http.StatusMethodNotAllowed}, {http.MethodPost, http.StatusNoContent}}

	for _, resendTest := range testResend {
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(resendTest.method, "", nil)
		request.SetBasicAuth("testerAdmin@test", "")

		handleResend(writer, request)
		if writer.statusCode != resendTest.expectedHTTPStatus {
			t.Errorf("handleResend returned a status of %d instead of %d\n", writer.statusCode, resendTest.expectedHTTPStatus)
		}
	}

	// shutdown
	testShutdown := []struct {
		appKey             string
		method             string
		expectedHTTPStatus int
	}{{"testerAdmin@test", http.MethodGet, http.StatusForbidden},
		{"testerSyncAdmin@test", http.MethodGet, http.StatusMethodNotAllowed}} // Note http.MethodPost isn't tested, would shutdown server

	for _, shutdownTest := range testShutdown {
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(shutdownTest.method, "", nil)
		request.SetBasicAuth(shutdownTest.appKey, "")

		handleShutdown(writer, request)
		if writer.statusCode != shutdownTest.expectedHTTPStatus {
			t.Errorf("handleResend returned a status of %d instead of %d\n", writer.statusCode, shutdownTest.expectedHTTPStatus)
		}
	}
}

func TestHandleObject(t *testing.T) {
	testHandleObjectHelper(common.CSS, common.Mongo, t)
	testHandleObjectHelper(common.CSS, common.Bolt, t)
	testHandleObjectHelper(common.ESS, common.InMemory, t)
	testHandleObjectHelper(common.ESS, common.Bolt, t)
}

func testHandleObjectHelper(nodeType string, storageType string, t *testing.T) {
	if status := testAPIServerSetup(nodeType, storageType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	testData := []struct {
		method              string
		appKey              string
		orgID               string
		objectType          string
		objectID            string
		operator            string
		metaData            *common.MetaData
		data                []byte
		expectedHTTPStatus  int
		webhook             *webhookUpdate
		destinationsList    *[]string
		expectedDeletedFlag bool
		testID              int
	}{
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 0},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 1},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "consumed", nil, nil, http.StatusBadRequest, nil, nil, false, 2},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "consumed", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 3},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 4},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 5},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 6},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "3", "status", nil, nil, http.StatusNotFound, nil, nil, false, 7},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 8},
		{http.MethodDelete, "testerAdmin@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusNoContent, nil, nil, false, 9},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 10},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 11},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "2", "", nil, nil, http.StatusNotFound, nil, nil, false, 12},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusOK, nil, nil, false, 13},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 14},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "2", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNotFound, nil, nil, false, 15},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1/2/3", "", nil, nil, http.StatusBadRequest, nil, nil, false, 16},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusMethodNotAllowed, nil, nil, false, 17},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "data", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 18},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "plover", nil, nil, http.StatusBadRequest, nil, nil, false, 19},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusOK, nil, nil, false, 20},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 21},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusBadRequest,
			&webhookUpdate{Action: "register", URL: "abc"}, nil, false, 22},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "received", nil, nil, http.StatusBadRequest, nil, nil, false, 23},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "received", nil, nil, http.StatusMethodNotAllowed, nil, nil, false, 24},

		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 25},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 26},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 27},

		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 28},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 29},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 30},

		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 31},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 32},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 33},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 34},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 35},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 36},
		{http.MethodDelete, "testerFail@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 37},
		{http.MethodDelete, "testerAdmin@myorg", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 38},
		{http.MethodDelete, "testerUser2@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 39},
		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 40},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 41},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 42},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, nil, nil, false, 43},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 44},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 45},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, nil, false, 46},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, nil, false, 47},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, nil, false, 48},
		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 49},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 50},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 51},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 52},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 53},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 54},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 55},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 56},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 57},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 58},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 59},
		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 60},
		{http.MethodDelete, "testerNode@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 61},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 62},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 63},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 64},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 65},
		{http.MethodGet, "testerUser%myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, nil, false, 66},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, nil, false, 67},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, &webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 51},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 68},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type2", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 69},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 70},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 71},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 72},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 73},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 74},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type2", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 75},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 76},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 77},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 78},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, nil, false, 79},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 80},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type2", "2", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 81},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 82},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 83},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 84},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 85},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 86},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type2", "2", "status", nil, nil, http.StatusOK, nil, nil, false, 87},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 88},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 89},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 90},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusForbidden, nil, nil, false, 91},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 92},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type2", "2", "destinations", nil, nil, http.StatusOK, nil, nil, false, 93},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 94},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 95},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusForbidden, nil, nil, false, 96},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusForbidden, nil, nil, false, 97},
		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusNoContent, nil, nil, false, 98},
		{http.MethodDelete, "testerNode@myorg222", "myorg222", "type2", "2", "", nil, nil, http.StatusNoContent, nil, nil, false, 99},
		{http.MethodDelete, "testerUser1@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 100},
		{http.MethodDelete, "testerNode1@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 101},
		{http.MethodDelete, "testerUser2@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 102},
		{http.MethodDelete, "testerNode2@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusForbidden, nil, nil, false, 103},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 104},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type2", "2", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 105},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 106},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 107},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 108},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusForbidden, nil, nil, false, 109},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusOK, nil, nil, false, 110},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusOK, nil, nil, false, 111},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusOK, nil, nil, false, 112},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusOK, nil, nil, false, 113},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 114},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusNotFound, nil, nil, false, 115},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 116},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 117},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 118},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 119},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusForbidden, nil, nil, false, 120},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type2", "100", "", nil, nil, http.StatusForbidden, nil, nil, false, 121},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 122},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 123},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 124},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 125},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 126},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 127},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 128},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type3", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 129},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type3", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 130},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type3", "4", "",
			&common.MetaData{ObjectID: "4", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 131},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type3", "5", "",
			&common.MetaData{ObjectID: "5", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 132},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type3", "6", "",
			&common.MetaData{ObjectID: "6", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 133},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 134},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type3", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 135},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type3", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 136},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type3", "4", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 137},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type3", "5", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 138},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type3", "6", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 139},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 140},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type3", "2", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 141},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type3", "3", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 142},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type3", "4", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 143},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type3", "5", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 144},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type3", "6", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 145},

		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 146},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type3", "2", "status", nil, nil, http.StatusOK, nil, nil, false, 147},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type3", "3", "status", nil, nil, http.StatusOK, nil, nil, false, 148},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type3", "4", "status", nil, nil, http.StatusOK, nil, nil, false, 149},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type3", "5", "status", nil, nil, http.StatusOK, nil, nil, false, 150},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type3", "6", "status", nil, nil, http.StatusOK, nil, nil, false, 151},

		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 152},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type3", "2", "destinations", nil, nil, http.StatusOK, nil, nil, false, 153},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type3", "3", "destinations", nil, nil, http.StatusOK, nil, nil, false, 154},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type3", "4", "destinations", nil, nil, http.StatusOK, nil, nil, false, 155},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type3", "5", "destinations", nil, nil, http.StatusOK, nil, nil, false, 156},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type3", "6", "destinations", nil, nil, http.StatusOK, nil, nil, false, 157},

		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type3", "1", "", nil, nil, http.StatusNoContent, nil, nil, false, 158},
		{http.MethodDelete, "testerUser1@myorg222", "myorg222", "type3", "2", "", nil, nil, http.StatusNoContent, nil, nil, false, 159},
		{http.MethodDelete, "testerUser2@myorg222", "myorg222", "type3", "3", "", nil, nil, http.StatusNoContent, nil, nil, false, 160},
		{http.MethodDelete, "testerNode@myorg222", "myorg222", "type3", "4", "", nil, nil, http.StatusNoContent, nil, nil, false, 161},
		{http.MethodDelete, "testerNode1@myorg222", "myorg222", "type3", "5", "", nil, nil, http.StatusNoContent, nil, nil, false, 162},
		{http.MethodDelete, "testerNode2@myorg222", "myorg222", "type3", "6", "", nil, nil, http.StatusNoContent, nil, nil, false, 163},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 164},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type3", "2", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 165},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type3", "3", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 166},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type3", "4", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 167},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type3", "5", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 168},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type3", "6", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 169},

		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 170},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 171},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 172},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 173},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 174},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, nil, false, 175},

		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 176},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 177},
		{http.MethodGet, "testerUser2@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 178},
		{http.MethodGet, "testerNode@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 179},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 180},
		{http.MethodGet, "testerNode2@myorg222", "myorg222", "type3", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 181},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 182},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 183},
		{http.MethodPut, "testerUser2@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 184},
		{http.MethodPut, "testerNode@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 185},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 186},
		{http.MethodPut, "testerNode2@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 187},

		{http.MethodPut, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 188},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type4", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 189},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type4", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 190},
		{http.MethodGet, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 191},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type4", "2", "",
			&common.MetaData{ObjectID: "2", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 192},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type4", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type4", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 193},
		{http.MethodPut, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 194},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type4", "2", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 195},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type4", "3", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 196},
		{http.MethodGet, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 197},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type4", "2", "status", nil, nil, http.StatusOK, nil, nil, false, 198},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type4", "3", "status", nil, nil, http.StatusOK, nil, nil, false, 199},
		{http.MethodGet, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 200},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type4", "2", "destinations", nil, nil, http.StatusOK, nil, nil, false, 201},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type4", "3", "destinations", nil, nil, http.StatusOK, nil, nil, false, 202},
		{http.MethodDelete, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "", nil, nil, http.StatusNoContent, nil, nil, false, 203},
		{http.MethodDelete, "testerUser1@myorg222", "myorg222", "type4", "2", "", nil, nil, http.StatusNoContent, nil, nil, false, 204},
		{http.MethodDelete, "testerNode1@myorg222", "myorg222", "type4", "3", "", nil, nil, http.StatusNoContent, nil, nil, false, 205},
		{http.MethodPut, "testerSyncAdmin@myorg222", "myorg222", "type4", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 206},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type4", "2", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 207},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type4", "3", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 208},
		{http.MethodGet, "testerSyncAdmin@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusOK, nil, nil, false, 209},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusOK, nil, nil, false, 210},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusOK, nil, nil, false, 211},
		{http.MethodGet, "testerSyncAdmin@myorg222", "myorg222", "type4", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 212},
		{http.MethodGet, "testerUser1@myorg222", "myorg222", "type4", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 213},
		{http.MethodGet, "testerNode1@myorg222", "myorg222", "type4", "100", "", nil, nil, http.StatusNotFound, nil, nil, false, 214},
		{http.MethodPut, "testerSyncAdmin@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 215},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 216},
		{http.MethodPut, "testerNode1@myorg222", "myorg222", "type4", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 217},

		{http.MethodPut, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type5", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 218},
		{http.MethodGet, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type5", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 219},
		{http.MethodPut, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 220},
		{http.MethodGet, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "status", nil, nil, http.StatusOK, nil, nil, false, 221},
		{http.MethodGet, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 222},
		{http.MethodDelete, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "", nil, nil, http.StatusNoContent, nil, nil, false, 223},
		{http.MethodPut, "testerSyncAdmin@myorg223", "myorg222", "type5", "1", "deleted", nil, nil, http.StatusNoContent, nil, nil, false, 224},
		{http.MethodGet, "testerSyncAdmin@myorg223", "myorg222", "type5", "", "", nil, nil, http.StatusOK, nil, nil, false, 225},
		{http.MethodGet, "testerSyncAdmin@myorg223", "myorg222", "type5", "2", "", nil, nil, http.StatusNotFound, nil, nil, false, 226},
		{http.MethodPut, "testerSyncAdmin@myorg223", "myorg222", "type5", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, nil, false, 227},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestinationsList: []string{"device: dev1>"}},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 228},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "destinations", nil,
			[]byte("abc"), http.StatusBadRequest, nil, &[]string{"<device: dev1", "device>: dev1"}, false, 229},

		{http.MethodPut, "myorg222$myserviceOrg$1.0.0$myservice1", "myorg222", "testESS", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type6", DestOrgID: "myorg222",
				DestinationPolicy: &common.Policy{
					Properties: []common.PolicyProperty{
						{Name: "j", Value: float64(42.0)},
						{Name: "k", Value: "ghjk"},
						{Name: "l", Value: float64(613)},
					},
					Constraints: []string{"il=71", "rtyu=\"edcrfv\""},
					Services: []common.ServiceID{
						{OrgID: "myserviceOrg", Arch: "amd64", ServiceName: "myservice1", Version: "1.0.0"},
					},
				},
			}, []byte("abc"), http.StatusNoContent, nil, nil, false, 94},
		{http.MethodGet, "myorg222$myserviceOrg$1.0.0$myservice1", "myorg222", "testESS", "", "", nil, nil, http.StatusNoContent, nil, nil, false, 230},
		{http.MethodGet, "myorg222$myserviceOrg$1.0.0$removed_service1", "myorg222", "testESS", "", "", nil, nil, http.StatusNoContent, nil, nil, true, 231},
		{http.MethodGet, "myorg222$myserviceOrg$1.0.0$not_exist_service", "myorg222", "testESS", "", "", nil, nil, http.StatusNotFound, nil, nil, true, 232},

		// test "public" object
		{http.MethodPut, "testerAdmin@publicOrg", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device", Public: true},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 233},

		{http.MethodGet, "testerAdmin@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 234},
		{http.MethodGet, "testerUser@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 235},
		{http.MethodGet, "testerUser@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 236},
		{http.MethodGet, "testerUser1@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 237},
		{http.MethodGet, "testerNode@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 238},
		{http.MethodGet, "testerNode1@myorg222", "publicOrg", "type_public", "public1", "",
			&common.MetaData{ObjectID: "public1", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, nil, false, 239},

		{http.MethodPut, "testerAdmin@publicOrg", "publicOrg", "type_public", "public1", "data", nil, []byte("updated"), http.StatusNoContent, nil, nil, false, 240},
		{http.MethodPut, "testerUser@myorg222", "publicOrg", "type_public", "public1", "data", nil, []byte("updated"), http.StatusForbidden, nil, nil, false, 241},
		{http.MethodPut, "testerUser1@myorg222", "publicOrg", "type_public", "public1", "data", nil, []byte("updated"), http.StatusForbidden, nil, nil, false, 242},
		{http.MethodPut, "testerNode@myorg222", "publicOrg", "type_public", "public1", "data", nil, []byte("updated"), http.StatusForbidden, nil, nil, false, 243},
		{http.MethodPut, "testerNode1@myorg222", "publicOrg", "type_public", "public1", "data", nil, []byte("updated"), http.StatusForbidden, nil, nil, false, 244},

		{http.MethodPut, "testerAdmin@publicOrg", "publicOrg", "type_public", "public2", "",
			&common.MetaData{ObjectID: "public2", ObjectType: "type_public", DestOrgID: "publicOrg", DestID: "dev1", DestType: "device", Public: true},
			[]byte("abc"), http.StatusNoContent, nil, nil, false, 245},
		{http.MethodDelete, "testerUser@myorg222", "publicOrg", "type_public", "public2", "", nil, nil, http.StatusForbidden, nil, nil, false, 246},
		{http.MethodDelete, "testerUser1@myorg222", "publicOrg", "type_public", "public2", "", nil, nil, http.StatusForbidden, nil, nil, false, 247},
		{http.MethodDelete, "testerNode@myorg222", "publicOrg", "type_public", "public2", "", nil, nil, http.StatusForbidden, nil, nil, false, 248},
		{http.MethodDelete, "testerNode1@myorg222", "publicOrg", "type_public", "public2", "", nil, nil, http.StatusForbidden, nil, nil, false, 249},
		{http.MethodDelete, "testerSyncAdmin@myorg222", "publicOrg", "type_public", "public2", "", nil, nil, http.StatusNoContent, nil, nil, false, 250},

		{http.MethodPut, "testerUser@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 251},
		{http.MethodPut, "testerNode@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 252},
		{http.MethodPut, "testerUser1@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 253},
		{http.MethodPut, "testerNode1@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 254},
		{http.MethodPut, "testerUser2@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 255},
		{http.MethodPut, "testerNode2@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusForbidden, nil, nil, false, 256},
		{http.MethodPut, "testerSyncAdmin@myorg222", "publicOrg", "type_public", "public1", "activate", nil, nil, http.StatusNoContent, nil, nil, false, 257},

		{http.MethodGet, "testerUser@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 258},
		{http.MethodGet, "testerNode@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 259},
		{http.MethodGet, "testerUser1@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 260},
		{http.MethodGet, "testerNode1@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 261},
		{http.MethodGet, "testerUser2@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 262},
		{http.MethodGet, "testerNode2@myorg222", "publicOrg", "type_public", "public1", "status", nil, nil, http.StatusOK, nil, nil, false, 263},

		{http.MethodGet, "testerUser@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 264},
		{http.MethodGet, "testerNode@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 265},
		{http.MethodGet, "testerUser1@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 266},
		{http.MethodGet, "testerNode1@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 267},
		{http.MethodGet, "testerUser2@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 268},
		{http.MethodGet, "testerNode2@myorg222", "publicOrg", "type_public", "public1", "destinations", nil, nil, http.StatusOK, nil, nil, false, 269},

		// test XSS in metadata
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Expiration: "<script>aaa</script>"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 270},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", ActivationTime: "01-01-01:<script>aaa</script>", Inactive: true},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 271},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Description: "Hello <script>aaa</script>"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 272},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Version: "Hello <script>aaa</script>"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 273},
		{http.MethodPut, "testerAdmin@myorg222", "my<org>222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Version: "1.0.0"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 274},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "ty=pe1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Version: "1.0.0"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 275},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "</1>", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device", Version: "1.0.0"},
			[]byte("abc"), http.StatusBadRequest, nil, nil, false, 276},
		{http.MethodGet, "testerAdmin@myorg222", "<myorg222", "type1", "", "", nil, nil, http.StatusBadRequest, nil, nil, false, 277},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "t=ype2", "1", "status", nil, nil, http.StatusBadRequest, nil, nil, false, 278},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1>", "destinations", nil, nil, http.StatusBadRequest, nil, nil, false, 279},
		{http.MethodPut, "testerUser@myorg222", "my==org222", "type1", "1", "received", nil, nil, http.StatusBadRequest, nil, nil, false, 280},
		{http.MethodDelete, "testerUser@myorg222", "<myorg222", "type3", "1", "", nil, nil, http.StatusBadRequest, nil, nil, false, 281},
		{http.MethodPut, "testerUser1@myorg222", "myorg222", "typ<e4>", "2", "deleted", nil, nil, http.StatusBadRequest, nil, nil, false, 282},
		{http.MethodPut, "testerAdmin@publicOrg", "publicOrg", "type==public", "public1", "data", nil, []byte("testXSS"), http.StatusBadRequest, nil, nil, false, 283},
	}

	destInfo := []struct {
		destType string
		destID   string
	}{
		{"device", "dev1"}, {"device2", "dev1"},
	}

	aclInfo := []struct {
		aclType string
		key     string
		user    common.ACLentry
	}{
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUser", ACLRole: security.ACLWriter}},
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUser1", ACLRole: security.ACLReader}},
		{"objects", "", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUser1", ACLRole: security.ACLWriter}},
		{"objects", "type3", common.ACLentry{ACLUserType: security.ACLUser, Username: "*", ACLRole: security.ACLWriter}},
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNode", ACLRole: security.ACLWriter}},
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNode1", ACLRole: security.ACLReader}},
		{"objects", "", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNode1", ACLRole: security.ACLWriter}},
		{"objects", "type3", common.ACLentry{ACLUserType: security.ACLNode, Username: "*", ACLRole: security.ACLWriter}},
		{"destinations", "*", common.ACLentry{ACLUserType: security.ACLUser, Username: "*", ACLRole: security.ACLWriter}},
		{"destinations", "*", common.ACLentry{ACLUserType: security.ACLNode, Username: "*", ACLRole: security.ACLWriter}},
	}

	for _, dest := range destInfo {
		if err := store.StoreDestination(common.Destination{DestOrgID: "myorg222", DestID: dest.destID, DestType: dest.destType}); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}
	}

	for _, dest := range destInfo {
		if err := store.StoreDestination(common.Destination{DestOrgID: "publicOrg", DestID: dest.destID, DestType: dest.destType}); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}
	}

	for _, info := range aclInfo {
		if err := store.AddUsersToACL(info.aclType, "myorg222", info.key, []common.ACLentry{info.user}); err != nil {
			t.Errorf("Failed to set up %s ACL. Error: %s\n", info.aclType, err.Error())
		}
	}

	for _, test := range testData {
		if common.Configuration.NodeType == common.CSS && test.objectType == "testESS" {
			continue
		}
		urlString := ""
		if common.Configuration.NodeType == common.CSS {
			urlString = urlString + test.orgID + "/"
		}
		urlString = urlString + test.objectType
		if len(test.objectID) != 0 {
			urlString = urlString + "/" + test.objectID
		}
		if len(test.operator) != 0 {
			urlString = urlString + "/" + test.operator
		}

		var buffer bytes.Buffer
		if test.method == http.MethodPut {
			encoder := json.NewEncoder(&buffer)
			if test.metaData != nil {
				payload := objectUpdate{Meta: *test.metaData}
				if err := encoder.Encode(payload); err != nil {
					t.Errorf("Failed to encode metaData. Error: %s\n", err)
				}
			} else if test.webhook != nil {
				if err := encoder.Encode(test.webhook); err != nil {
					t.Errorf("Failed to encode webhook. Error: %s\n", err)
				}
			} else if test.operator == "destinations" {
				if err := encoder.Encode(test.destinationsList); err != nil {
					t.Errorf("Failed to encode destinationsList. Error: %s\n", err)
				}
			}
		}

		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(test.method, urlString, ioutil.NopCloser(&buffer))
		request.SetBasicAuth(test.appKey, "")

		handleObjects(writer, request)
		if writer.statusCode == test.expectedHTTPStatus {
			if test.data != nil {
				if common.Configuration.NodeType == common.CSS {
					urlString = test.orgID + "/"
				} else {
					urlString = ""
				}
				urlString = urlString + test.objectType + "/" + test.objectID + "/data"
				var buffer bytes.Buffer
				if test.method == http.MethodPut {
					buffer.Write(test.data)
				}
				writer := newAPIServerTestResponseWriter()
				request, _ := http.NewRequest(test.method, urlString, ioutil.NopCloser(&buffer))
				request.SetBasicAuth(test.appKey, "")

				handleObjects(writer, request)
				if writer.statusCode == test.expectedHTTPStatus {
					if test.method == http.MethodGet && test.expectedHTTPStatus == http.StatusOK {
						if bytes.Compare(test.data, writer.body.Bytes()) != 0 {
							t.Errorf("handleObjects of %s returned \"%s\" instead of \"%s\"\n", urlString, string(writer.body.Bytes()), string(test.data))
						}
					}
				} else if nodeType != common.ESS && test.method != "destinations" {
					t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d and %s\n", urlString,
						writer.statusCode, test.expectedHTTPStatus, test.testID, nodeType)
				}
			} else if test.objectType == "testESS" {
				// only for ESS
				if test.method == http.MethodPut {
					removedServices := []common.ServiceID{
						{OrgID: "myserviceOrg", Arch: "amd64", ServiceName: "removed_service1", Version: "1.0.0"},
						{OrgID: "myserviceOrg", Arch: "amd64", ServiceName: "removed_service2", Version: "1.0.0"},
						{OrgID: "myserviceOrg", Arch: "amd64", ServiceName: "removed_service3", Version: "1.0.0"},
					}

					if err := store.UpdateRemovedDestinationPolicyServices("myorg222", "testESS", "1", removedServices); err != nil {
						t.Errorf("Failed to store removedServices for ESS. Error: %s\n", err.Error())
					}

				} else if test.method == http.MethodGet {
					// test deleted flag for different auth
					responseMetas := []common.MetaData{}
					json.Unmarshal(writer.body.Bytes(), responseMetas)
					for _, responseMeta := range responseMetas {
						if responseMeta.ObjectType == test.objectType && responseMeta.ObjectID == test.objectID {
							if responseMeta.Deleted != test.expectedDeletedFlag {
								t.Errorf("Got unexpeced deleted flag for test: %d", test.testID)
							}

						}
					}
				}
			}
		} else if nodeType != common.ESS && test.method != "destinations" {
			t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d and %s under %s and %s database\n", urlString,
				writer.statusCode, test.expectedHTTPStatus, test.testID, nodeType, test.appKey, storageType)
		}
	}

	for _, info := range aclInfo {
		if err := store.RemoveUsersFromACL(info.aclType, "myorg222", info.key, []common.ACLentry{info.user}); err != nil {
			t.Errorf("Failed to cleanup %s ACL. Error: %s\n", info.aclType, err.Error())
		}
	}

	if nodeType == common.CSS {
		if err := deleteOrganization("myorg222"); err != nil {
			t.Errorf("deleteOrganization failed. Error: %s", err.Error())
		}
	}
}

func TestInvalidURLs(t *testing.T) {
	testInvalidURLs(common.Bolt, t)
	testInvalidURLs(common.Mongo, t)
}

func testInvalidURLs(storageType string, t *testing.T) {
	if status := testAPIServerSetup(common.CSS, storageType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()
	defer security.Stop()

	writer := newAPIServerTestResponseWriter()
	request, _ := http.NewRequest(http.MethodPut, "", nil)
	request.SetBasicAuth("testerAdmin@myorg", "")
	handleObjects(writer, request)
	if writer.statusCode != http.StatusBadRequest {
		t.Errorf("handleObjects of \"\" returned a status of %d instead of %d\n", writer.statusCode, http.StatusBadRequest)
	}

	writer = newAPIServerTestResponseWriter()
	request, _ = http.NewRequest(http.MethodPut, "plover", nil)
	request.SetBasicAuth("testerAdmin@plover", "")
	handleObjects(writer, request)
	if writer.statusCode != http.StatusMethodNotAllowed {
		t.Errorf("handleObjects of \"plover\" returned a status of %d instead of %d\n", writer.statusCode, http.StatusMethodNotAllowed)
	}

	writer = newAPIServerTestResponseWriter()
	request, _ = http.NewRequest(http.MethodGet, "plover", nil)
	request.SetBasicAuth("testerAdmin@plover", "")
	handleObjects(writer, request)
	if writer.statusCode != http.StatusBadRequest {
		t.Errorf("handleObjects of \"plover\" returned a status of %d instead of %d\n", writer.statusCode, http.StatusBadRequest)
	}

	writer = newAPIServerTestResponseWriter()
	request, _ = http.NewRequest(http.MethodPut, "myorg/mytype/1/data", nil)
	request.SetBasicAuth("testerAdmin@myorg", "")
	handleObjects(writer, request)
	if writer.statusCode != http.StatusNotFound {
		t.Errorf("handleObjects of \"myorg/mytype/1/data\" returned a status of %d instead of %d\n", writer.statusCode, http.StatusNotFound)
	}

	writer = newAPIServerTestResponseWriter()
	request, _ = http.NewRequest(http.MethodPut, "/api/v1/shutdown", nil)
	request.SetBasicAuth("testerSyncAdmin@myorg", "")
	handleShutdown(writer, request)
	if writer.statusCode != http.StatusMethodNotAllowed {
		t.Errorf("handleObjects of \"/api/v1/shutdown\" returned a status of %d instead of %d\n", writer.statusCode, http.StatusMethodNotAllowed)
	}
}

func TestPolicies(t *testing.T) {
	if status := testAPIServerSetup(common.ESS, common.InMemory); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	common.Configuration.OrgID = "myorgPolicy"

	_, _, err := loadTestPolicyData(common.ESS, "myorgPolicy")
	if err != nil {
		t.Errorf("StoreObject failed: %s", err.Error())
	}

	tests := []struct {
		method             string
		appKey             string
		orgID              string
		objectType         string
		objectID           string
		expectedHTTPStatus int
		expectedCount      int
		testID             int
	}{
		{http.MethodGet, "testerService1@myorgPolicy", "myorgPolicy", "type1", "1", http.StatusOK, 0, 1},
		{http.MethodGet, "testerService2@myorgPolicy", "myorgPolicy", "type1", "1", http.StatusForbidden, 0, 2},
		{http.MethodGet, "kuku@myorgPolicy", "myorgPolicy", "type1", "1", http.StatusForbidden, 0, 3},
		{http.MethodGet, "testerUser@myorgPolicy", "myorgPolicy", "type1", "1", http.StatusOK, 0, 4},

		{http.MethodGet, "testerService1@myorgPolicy", "myorgPolicy", "type1", "2", http.StatusOK, 0, 5},
		{http.MethodGet, "testerService2@myorgPolicy", "myorgPolicy", "type1", "2", http.StatusOK, 0, 6},
		{http.MethodGet, "testerService2b@myorgPolicy", "myorgPolicy", "type1", "2", http.StatusForbidden, 0, 7},
		{http.MethodGet, "kuku@myorgPolicy", "myorgPolicy", "type1", "2", http.StatusForbidden, 0, 8},
		{http.MethodGet, "testerUser@myorgPolicy", "myorgPolicy", "type1", "2", http.StatusOK, 0, 9},

		{http.MethodGet, "testerService2@myorgPolicy", "myorgPolicy", "type1", "2a", http.StatusForbidden, 0, 10},
		{http.MethodGet, "testerService2b@myorgPolicy", "myorgPolicy", "type1", "2a", http.StatusOK, 0, 11},

		{http.MethodGet, "testerService1@myorgPolicy", "myorgPolicy", "type1", "3", http.StatusForbidden, 0, 12},
		{http.MethodGet, "testerService2@myorgPolicy", "myorgPolicy", "type1", "3", http.StatusForbidden, 0, 13},
		{http.MethodGet, "kuku@myorgPolicy", "myorgPolicy", "type1", "3", http.StatusForbidden, 0, 14},
		{http.MethodGet, "testerUser@myorgPolicy", "myorgPolicy", "type1", "3", http.StatusOK, 0, 15},

		{http.MethodGet, "testerService1@myorgPolicy", "myorgPolicy", "type1", "", http.StatusOK, 2, 16},
		{http.MethodGet, "testerService2@myorgPolicy", "myorgPolicy", "type1", "", http.StatusOK, 1, 17},
		{http.MethodGet, "testerService2b@myorgPolicy", "myorgPolicy", "type1", "", http.StatusOK, 1, 18},
		{http.MethodGet, "kuku@myorgPolicy", "myorgPolicy", "type1", "", http.StatusForbidden, 0, 19},
		{http.MethodGet, "testerUser@myorgPolicy", "myorgPolicy", "type1", "", http.StatusOK, 4, 20},
	}
	for _, test := range tests {
		urlString := test.objectType
		if test.objectID != "" {
			urlString = urlString + "/" + test.objectID
		}

		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(test.method, urlString, nil)
		request.SetBasicAuth(test.appKey, "")

		handleObjects(writer, request)
		if writer.statusCode != test.expectedHTTPStatus {
			t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d under %s\n",
				urlString, writer.statusCode, test.expectedHTTPStatus, test.testID, test.appKey)
		}
		if writer.statusCode == http.StatusOK && test.objectID == "" {
			decoder := json.NewDecoder(&writer.body)
			var data []common.MetaData
			if err := decoder.Decode(&data); err != nil {
				t.Errorf("Failed to unmarshall objects. Error: %s\n", err)
			} else {
				if len(data) != test.expectedCount {
					t.Errorf("Fetched %d objects instead of %d for test %d under %s",
						len(data), test.expectedCount, test.testID, test.appKey)
				}
			}
		}
	}
}

func TestObjectsWithPolicyUpdatedSince(t *testing.T) {
	testObjectsWithPolicyUpdatedSinceHelper(common.Mongo, t)
	testObjectsWithPolicyUpdatedSinceHelper(common.Bolt, t)
}

func testObjectsWithPolicyUpdatedSinceHelper(storageProvider string, t *testing.T) {
	if status := testAPIServerSetup(common.CSS, storageProvider); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	since, totalCount, err := loadTestPolicyData(common.CSS, "")
	if err != nil {
		t.Errorf("StoreObject failed: %s", err.Error())
	}

	tests := []struct {
		method             string
		orgID              string
		since              int64
		expectedHTTPStatus int
		expectedCount      int
		testID             int
	}{
		// Must be first test
		{http.MethodGet, "myorgPolicy", -1, http.StatusOK, 1, 0},
		// Must be second test
		{http.MethodGet, "myorgPolicy", 1, http.StatusOK, -1, 1},
		{http.MethodGet, "myorgPolicy1", 1, http.StatusOK, 1, 2},
		{http.MethodGet, "myorgPolicy", 0, http.StatusBadRequest, 0, 3},
		{http.MethodPut, "myorgPolicy", 1, http.StatusMethodNotAllowed, 0, 4},
		{http.MethodGet, "myorgPolicy2", 1, http.StatusNotFound, 0, 5},
	}

	tests[0].since = since
	tests[1].expectedCount = totalCount - 1

	for _, test := range tests {
		urlString := fmt.Sprintf("%s?destination_policy=true&since=%d", test.orgID, test.since)
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(test.method, urlString, nil)
		request.SetBasicAuth("testerAdmin@"+test.orgID, "")

		handleObjects(writer, request)
		if writer.statusCode != test.expectedHTTPStatus {
			t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d\n",
				urlString, writer.statusCode, test.expectedHTTPStatus, test.testID)
		}
		if writer.statusCode == http.StatusOK {
			decoder := json.NewDecoder(&writer.body)
			var data []common.ObjectDestinationPolicy
			if err := decoder.Decode(&data); err != nil {
				t.Errorf("Failed to unmarshall objects. Error: %s\n", err)
			} else {
				if len(data) != test.expectedCount {
					t.Errorf("Fetched %d objects instead of %d for test %d",
						len(data), test.expectedCount, test.testID)
				}
			}
		}
	}
}

func loadTestPolicyData(nodeType string, orgID string) (int64, int, error) {
	testData := []common.MetaData{
		common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorgPolicy1", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
				},
			},
		},
		common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorgPolicy", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
				},
			},
		},
		common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorgPolicy", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService2", Version: "[0.0.1, 0.1.0)"},
				},
			},
		},
		common.MetaData{ObjectID: "2a", ObjectType: "type1", DestOrgID: "myorgPolicy", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService2", Version: "[0.1.0, INFINITY)"},
				},
			},
		},
		common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorgPolicy", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService3", Version: "0.0.1"},
				},
			},
		},
	}

	var since int64

	for _, metaData := range testData {
		if nodeType == common.ESS && metaData.DestOrgID != orgID {
			continue
		}
		since = time.Now().UTC().UnixNano()
		time.Sleep(10 * time.Millisecond)

		if _, err := store.StoreObject(metaData, nil, common.CompletelyReceived); err != nil {
			return 0, 0, err
		}
	}

	return since, len(testData), nil
}

func TestGetObjectsWithFilters(t *testing.T) {
	testGetObjectsWithFiltersHelper(common.Mongo, t)
	testGetObjectsWithFiltersHelper(common.Bolt, t)
}

func testGetObjectsWithFiltersHelper(storageProvider string, t *testing.T) {
	if status := testAPIServerSetup(common.CSS, storageProvider); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	_, _, err := loadTestMetaData(common.CSS, "")
	if err != nil {
		t.Errorf("StoreObject failed: %s", err.Error())
	}

	destinations := []common.Destination{
		common.Destination{DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5a", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5b", Communication: common.MQTTProtocol},
		common.Destination{DestOrgID: "publicOrgObjFilter", DestType: "testPublicDestType", DestID: "testPublicDestID", Communication: common.MQTTProtocol},
	}

	for _, destination := range destinations {
		if err := store.StoreDestination(destination); err != nil {
			t.Errorf("Failed to store detination %#v. Error: %s\n", destination, err)
		}
	}

	tests := []struct {
		method             string
		appKey             string
		orgID              string
		destinationPolicy  string
		destService        string
		destPropname       string
		since              string
		objType            string
		objID              string
		destType           string
		destID             string
		noData             string
		expirationBefore   string
		expectedHTTPStatus int
		expectedCount      int
		testID             int
	}{
		// Must be first test
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "", "", "", "", http.StatusOK, 12, 0},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "", "", "", "", "", "", "", http.StatusOK, 6, 1},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "false", "", "", "", "", "", "", "", "", "", http.StatusOK, 6, 2},
		// Must be second test
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "plover/testerService1", "", "", "", "", "", "", "", "", http.StatusOK, 2, 3},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "plover/testerService1", "b", "", "", "", "", "", "", "", http.StatusOK, 1, 4},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "plover/testerService1", "b", "", "", "", "", "", "", "", http.StatusOK, 12, 5},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "b", "", "", "", "", "", "", "", http.StatusOK, 5, 6},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "2000-08-14T14:00:00Z", "", "", "", "", "", "", http.StatusOK, 6, 7},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "2030-08-14T14:00:00Z", "", "", "", "", "", "", http.StatusNotFound, 0, 8},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "", "", "", "", "", "false", "", http.StatusOK, 2, 9},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "", "", "", "", "", "true", "", http.StatusOK, 4, 10},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "false", "", "", "", "", "", "", "", "false", "", http.StatusOK, 4, 11},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "false", "", "", "", "", "", "", "", "true", "", http.StatusOK, 2, 12},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "", "", "false", "", http.StatusOK, 6, 13},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "", "", "true", "", http.StatusOK, 6, 14},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "myDestType5", "", "", "", http.StatusOK, 6, 15},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "myDestType5", "myDestID5a", "", "", http.StatusOK, 4, 16},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 3, 17},
		{http.MethodGet, "testerUser@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 3, 18},
		{http.MethodGet, "testerUser1@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 0, 19},
		{http.MethodGet, "testerUser2@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 0, 20},
		{http.MethodGet, "testerUserWrong@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusForbidden, 3, 21},
		{http.MethodGet, "testerUserCanAccessAllTypes@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 3, 22},

		{http.MethodGet, "testerNode@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 3, 23},
		{http.MethodGet, "testerNode1@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 0, 24},
		{http.MethodGet, "testerNode2@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 0, 25},
		{http.MethodGet, "testerNodeWrong@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusForbidden, 3, 26},
		{http.MethodGet, "testerNodeCanAccessAllTypes@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "", "", "", http.StatusOK, 3, 27},

		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "", "myDestType5", "myDestID5a", "", "", http.StatusOK, 2, 28},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "type2", "7c", "myDestType5", "myDestID5a", "", "", http.StatusOK, 1, 29},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "", "", "", "", "", "", "", "", "", "2012-08-15T14:00:00Z", http.StatusOK, 2, 30},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "false", "", "", "", "", "", "", "", "", "2012-08-15T14:00:00Z", http.StatusNotFound, 0, 31},
		{http.MethodGet, "testerAdmin@myorgObjFilter1", "myorgObjFilter1", "true", "", "", "", "", "", "", "", "", "", http.StatusOK, 1, 32},
		{http.MethodGet, "testerAdmin@myorgObjFilter", "myorgObjFilter", "aaa", "", "", "", "", "", "", "", "", "", http.StatusBadRequest, 0, 33},
		{http.MethodPut, "testerAdmin@myorgObjFilter", "myorgObjFilter", "true", "", "", "", "", "", "", "", "", "", http.StatusMethodNotAllowed, 0, 34},
		{http.MethodGet, "testerAdmin@myorgObjFilter2", "myorgObjFilter2", "true", "", "", "", "", "", "", "", "", "", http.StatusNotFound, 0, 35},

		{http.MethodGet, "testerNode@myorgObjFilter", "publicOrgObjFilter", "", "", "", "", "type_public", "", "", "", "", "", http.StatusOK, 1, 36},
	}

	aclInfo := []struct {
		aclType string
		key     string
		user    common.ACLentry
	}{
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUser", ACLRole: security.ACLReader}},
		{"objects", "type1", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUser1", ACLRole: security.ACLWriter}},
		{"objects", "*", common.ACLentry{ACLUserType: security.ACLUser, Username: "testerUserCanAccessAllTypes", ACLRole: security.ACLWriter}},
		{"objects", "type2", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNode", ACLRole: security.ACLReader}},
		{"objects", "type1", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNode1", ACLRole: security.ACLWriter}},
		{"objects", "*", common.ACLentry{ACLUserType: security.ACLNode, Username: "testerNodeCanAccessAllTypes", ACLRole: security.ACLWriter}},
	}

	for _, info := range aclInfo {
		if err := store.AddUsersToACL(info.aclType, "myorgObjFilter", info.key, []common.ACLentry{info.user}); err != nil {
			t.Errorf("Failed to set up %s ACL. Error: %s\n", info.aclType, err.Error())
		}
	}

	for _, test := range tests {
		urlString := fmt.Sprintf("%s?filters=true&destinationPolicy=%s&dpPropertyName=%s&dpService=%s&since=%s&objectType=%s&objectID=%s&destinationType=%s&destinationID=%s&noData=%s&expirationTimeBefore=%s",
			test.orgID, test.destinationPolicy, test.destPropname, test.destService, test.since, test.objType, test.objID, test.destType, test.destID, test.noData, test.expirationBefore)
		writer := newAPIServerTestResponseWriter()
		request, _ := http.NewRequest(test.method, urlString, nil)
		//request.SetBasicAuth("testerAdmin@"+test.orgID, "")
		request.SetBasicAuth(test.appKey, "")

		handleObjects(writer, request)
		if writer.statusCode != test.expectedHTTPStatus {
			t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d under %s and %s database\n",
				urlString, writer.statusCode, test.expectedHTTPStatus, test.testID, test.appKey, storageProvider)
		}
		if writer.statusCode == http.StatusOK {
			decoder := json.NewDecoder(&writer.body)
			var data []common.ObjectDestinationPolicy
			if err := decoder.Decode(&data); err != nil {
				t.Errorf("Failed to unmarshall objects. Error: %s\n", err)
			} else {
				if len(data) != test.expectedCount {
					t.Errorf("Fetched %d objects instead of %d for test %d",
						len(data), test.expectedCount, test.testID)
				}
			}
		}
	}
}

func loadTestMetaData(nodeType string, orgID string) (int64, int, error) {
	destArray := []string{"myDestType5:myDestID5a", "myDestType5:myDestID5c"}
	testData := []common.MetaData{
		common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorgObjFilter1", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
				},
			},
			Expiration: "",
		},
		common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorgObjFilter", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
				},
			},
			Expiration: "2011-08-14T14:00:00Z",
		},
		common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorgObjFilter", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService1", Version: "0.0.1"},
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService2", Version: "[0.0.1, 0.1.0)"},
				},
			},
			Expiration: "",
		},
		common.MetaData{ObjectID: "2a", ObjectType: "type1", DestOrgID: "myorgObjFilter", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService2", Version: "[0.1.0, INFINITY)"},
				},
			},
			Expiration: "2012-08-14T14:00:00Z",
		},
		common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorgObjFilter", NoData: true,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService3", Version: "0.0.1"},
				},
			},
			Expiration: "2013-08-14T14:00:00Z",
		},
		common.MetaData{ObjectID: "4", ObjectType: "type1", DestOrgID: "myorgObjFilter", NoData: false,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService4", Version: "0.0.1"},
				},
			},
			Expiration: "2014-08-14T14:00:00Z",
		},
		common.MetaData{ObjectID: "5a", ObjectType: "type1", DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5a", NoData: true, Expiration: "2015-08-14T14:00:00Z"},
		common.MetaData{ObjectID: "5b", ObjectType: "type1", DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5b", NoData: false, Expiration: "2015-08-14T14:00:00Z"},
		common.MetaData{ObjectID: "5c", ObjectType: "type1", DestOrgID: "myorgObjFilter", DestinationsList: destArray, NoData: false, Expiration: "2015-08-14T14:00:00Z"},
		common.MetaData{ObjectID: "6", ObjectType: "type2", DestOrgID: "myorgObjFilter", NoData: false,
			DestinationPolicy: &common.Policy{
				Properties: []common.PolicyProperty{
					{Name: "a", Value: float64(1)},
					{Name: "b", Value: "zxcv"},
					{Name: "c", Value: true, Type: "bool"},
				},
				Constraints: []string{"Plover=34", "asdf=true"},
				Services: []common.ServiceID{
					{OrgID: "plover", Arch: "amd64", ServiceName: "testerService4", Version: "0.0.1"},
				},
			},
			Expiration: "2014-08-14T14:00:00Z",
		},
		common.MetaData{ObjectID: "7a", ObjectType: "type2", DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5a", NoData: true, Expiration: "2015-08-14T14:00:00Z"},
		common.MetaData{ObjectID: "7b", ObjectType: "type2", DestOrgID: "myorgObjFilter", DestType: "myDestType5", DestID: "myDestID5b", NoData: false, Expiration: "2015-08-14T14:00:00Z"},
		common.MetaData{ObjectID: "7c", ObjectType: "type2", DestOrgID: "myorgObjFilter", DestinationsList: destArray, NoData: false, Expiration: "2015-08-14T14:00:00Z"},

		common.MetaData{ObjectID: "8", ObjectType: "type_public", DestOrgID: "publicOrgObjFilter", DestType: "testPublicDestType", DestID: "testPublicDestID", NoData: true, Expiration: "2015-08-14T14:00:00Z", Public: true},
	}

	var since int64

	for _, metaData := range testData {
		if nodeType == common.ESS && metaData.DestOrgID != orgID {
			continue
		}
		since = time.Now().UTC().UnixNano()
		time.Sleep(10 * time.Millisecond)

		if _, err := store.StoreObject(metaData, nil, common.CompletelyReceived); err != nil {
			return 0, 0, err
		}
	}

	return since, len(testData), nil
}

func testAPIServerSetup(nodeType string, storageType string) string {
	common.Running = true
	time.Sleep(100 * time.Millisecond) // Wait a bit

	if storageType == "" {
		if nodeType == common.CSS {
			storageType = common.Mongo
		} else {
			storageType = common.Bolt
		}
	}
	switch storageType {
	case common.Mongo:
		common.Configuration.MongoDbName = "d_test_db"
		communications.Store = &storage.MongoStorage{}
	case common.InMemory:
		communications.Store = &storage.InMemoryStorage{}
	case common.Bolt:
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup(true)
		communications.Store = boltStore
	}

	store = communications.Store
	if err := store.Init(); err != nil {
		return fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}

	common.InitObjectLocks()

	security.SetAuthentication(&security.TestAuthenticate{})
	security.Store = store
	security.Start()

	communications.Comm = &communications.TestComm{}
	if err := communications.Comm.StartCommunication(); err != nil {
		return fmt.Sprintf("Failed to start MQTT communication. Error: %s", err.Error())
	}

	common.Configuration.NodeType = nodeType
	return ""
}

type apiServerTestResponseWriter struct {
	statusCode int
	header     http.Header
	body       bytes.Buffer
}

func newAPIServerTestResponseWriter() *apiServerTestResponseWriter {
	writer := new(apiServerTestResponseWriter)
	writer.header = make(map[string][]string)
	return writer
}

func (writer *apiServerTestResponseWriter) Header() http.Header {
	return writer.header
}

func (writer *apiServerTestResponseWriter) Write(p []byte) (int, error) {
	return writer.body.Write(p)
}

func (writer *apiServerTestResponseWriter) WriteHeader(statusCode int) {
	writer.statusCode = statusCode
}
