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
	if status := testAPIServerSetup(common.CSS); status != "" {
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
	if status := testAPIServerSetup(common.CSS); status != "" {
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
	if status := testAPIServerSetup(common.ESS); status != "" {
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
	testHandleObjectHelper(common.CSS, t)
	testHandleObjectHelper(common.ESS, t)
}

func testHandleObjectHelper(nodeType string, t *testing.T) {
	if status := testAPIServerSetup(nodeType); status != "" {
		t.Errorf(status)
	}
	defer communications.Store.Stop()

	testData := []struct {
		method             string
		appKey             string
		orgID              string
		objectType         string
		objectID           string
		operator           string
		metaData           *common.MetaData
		data               []byte
		expectedHTTPStatus int
		webhook            *webhookUpdate
		testID             int
	}{
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNoContent, nil, 0},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, 1},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "consumed", nil, nil, http.StatusBadRequest, nil, 2},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "consumed", nil, nil, http.StatusMethodNotAllowed, nil, 3},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusNoContent, nil, 4},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusMethodNotAllowed, nil, 5},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusOK, nil, 6},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "3", "status", nil, nil, http.StatusNotFound, nil, 7},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusMethodNotAllowed, nil, 8},
		{http.MethodDelete, "testerAdmin@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusNoContent, nil, 9},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusNoContent, nil, 10},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusMethodNotAllowed, nil, 11},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "2", "", nil, nil, http.StatusNotFound, nil, 12},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusOK, nil, 13},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusMethodNotAllowed, nil, 14},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "2", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNotFound, nil, 15},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1/2/3", "", nil, nil, http.StatusBadRequest, nil, 16},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "",
			&common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusMethodNotAllowed, nil, 17},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "data", nil, nil, http.StatusMethodNotAllowed, nil, 18},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "3", "plover", nil, nil, http.StatusBadRequest, nil, 19},
		{http.MethodGet, "testerAdmin@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusOK, nil, 20},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, 21},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusBadRequest,
			&webhookUpdate{Action: "register", URL: "abc"}, 22},
		{http.MethodPut, "testerAdmin@myorg222", "myorg222", "type1", "1", "received", nil, nil, http.StatusBadRequest, nil, 23},
		{http.MethodPost, "testerAdmin@myorg222", "myorg222", "type1", "1", "received", nil, nil, http.StatusMethodNotAllowed, nil, 24},

		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 25},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 26},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 27},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 28},
		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, 29},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, 30},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, 31},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, 32},
		{http.MethodDelete, "testerFailn@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, 33},
		{http.MethodDelete, "testerAdmin@myorg", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, 34},
		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, 35},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, 36},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, nil, 37},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, nil, 38},
		{http.MethodGet, "testerFail@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, 39},
		{http.MethodGet, "testerAdmin@myorg", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, 40},
		{http.MethodPut, "testerFail@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, 41},
		{http.MethodPut, "testerAdmin@myorg", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden,
			&webhookUpdate{Action: "register", URL: "http://abc"}, 42},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 43},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusForbidden, nil, 44},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "activate", nil, nil, http.StatusForbidden, nil, 45},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "1", "status", nil, nil, http.StatusForbidden, nil, 46},
		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type1", "1", "", nil, nil, http.StatusForbidden, nil, 47},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "1", "deleted", nil, nil, http.StatusForbidden, nil, 48},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, nil, 49},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type1", "1", "destinations",
			&common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			nil, http.StatusForbidden, nil, 50},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type1", "", "", nil, nil, http.StatusForbidden, &webhookUpdate{Action: "register", URL: "http://abc"}, 51},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusNoContent, nil, 53},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg222", DestID: "dev1", DestType: "device"},
			[]byte("abc"), http.StatusOK, nil, 53},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "activate", nil, nil, http.StatusNoContent, nil, 54},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "status", nil, nil, http.StatusOK, nil, 55},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "1", "destinations", nil, nil, http.StatusOK, nil, 56},
		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type2", "1", "", nil, nil, http.StatusNoContent, nil, 57},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "1", "deleted", nil, nil, http.StatusNoContent, nil, 58},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusOK, nil, 59},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type2", "2", "", nil, nil, http.StatusNotFound, nil, 60},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type2", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, 61},

		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusNoContent, nil, 62},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "",
			&common.MetaData{ObjectID: "1", ObjectType: "type3", DestOrgID: "myorg222", DestID: "dev1", DestType: "device2"},
			[]byte("abc"), http.StatusOK, nil, 63},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "activate", nil, nil, http.StatusNoContent, nil, 64},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "status", nil, nil, http.StatusOK, nil, 65},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "1", "destinations", nil, nil, http.StatusOK, nil, 66},
		{http.MethodDelete, "testerUser@myorg222", "myorg222", "type3", "1", "", nil, nil, http.StatusNoContent, nil, 67},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "1", "deleted", nil, nil, http.StatusNoContent, nil, 68},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusOK, nil, 69},
		{http.MethodGet, "testerUser@myorg222", "myorg222", "type3", "2", "", nil, nil, http.StatusNotFound, nil, 70},
		{http.MethodPut, "testerUser@myorg222", "myorg222", "type3", "", "", nil, nil, http.StatusNoContent,
			&webhookUpdate{Action: "register", URL: "http://abc"}, 71},
	}

	destInfo := []struct {
		destType string
		destID   string
	}{
		{"device", "dev1"}, {"device2", "dev1"},
	}

	aclInfo := []struct {
		aclType  string
		key      string
		username string
	}{
		{"objects", "type2", "testerUser"}, {"objects", "type3", "*"},
		{"destinations", "device", "testerUser"}, {"destinations", "device2", "*"},
	}

	for _, dest := range destInfo {
		if err := store.StoreDestination(common.Destination{DestOrgID: "myorg222", DestID: dest.destID, DestType: dest.destType}); err != nil {
			t.Errorf("StoreDestination failed. Error: %s\n", err.Error())
		}
	}

	for _, info := range aclInfo {
		if err := store.AddUsersToACL(info.aclType, "myorg222", info.key, []string{info.username}); err != nil {
			t.Errorf("Failed to set up %s ACL. Error: %s\n", info.aclType, err.Error())
		}
	}

	for _, test := range testData {
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
			}
		} else if nodeType != common.ESS && test.method != "destinations" {
			t.Errorf("handleObjects of %s returned a status of %d instead of %d for test %d and %s under %s\n", urlString, writer.statusCode,
				test.expectedHTTPStatus, test.testID, nodeType, test.appKey)
		}
	}

	for _, info := range aclInfo {
		if err := store.RemoveUsersFromACL(info.aclType, "myorg222", info.key, []string{info.username}); err != nil {
			t.Errorf("Failed to cleanup %s ACL. Error: %s\n", info.aclType, err.Error())
		}
	}
}

func TestInvalidURLs(t *testing.T) {
	if status := testAPIServerSetup(common.CSS); status != "" {
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

func testAPIServerSetup(nodeType string) string {
	common.Running = true
	time.Sleep(100 * time.Millisecond) // Wait a bit

	if nodeType == common.CSS {
		communications.Store = &storage.MongoStorage{}
	} else {
		//communications.Store = &storage.InMemoryStorage{}
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup()
		communications.Store = boltStore
	}
	store = communications.Store
	if err := store.Init(); err != nil {
		return fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}

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
