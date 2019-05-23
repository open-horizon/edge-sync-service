package communications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service/core/security"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
)

var httpComm *HTTP

type httpTestObjectInfo struct {
	metaData common.MetaData
	status   string
	data     []byte
}

func TestHTTPCommUpdatedObjects(t *testing.T) {
	if status := testHTTPCommSetup("CSS"); status != "" {
		t.Errorf(status)
	}
	defer Store.Stop()
	defer security.Stop()

	destination := common.Destination{DestOrgID: "myorg000", DestID: "dev1", DestType: "httpDevice", Communication: common.HTTPProtocol}
	if err := Store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	testObjects := []httpTestObjectInfo{
		{common.MetaData{ObjectID: "1", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "httpDevice"},
			common.ReadyToSend, []byte("plokmijnuhbygv")},
		{common.MetaData{ObjectID: "2", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "httpDevice",
			Deleted: true},
			common.ObjDeleted, nil},
		{common.MetaData{ObjectID: "3", ObjectType: "type1", DestOrgID: "myorg000", DestID: "dev1", DestType: "httpDevice",
			Version: "123", Description: "abc", Inactive: true},
			common.NotReadyToSend, nil},
	}

	readyToSend := 0
	for _, testObject := range testObjects {
		if testObject.status == common.ReadyToSend || testObject.status == common.DeletePending {
			readyToSend++
		}
	}

	testLoadObjects(testObjects, t)

	registerTestDevice("myorg000", "httpDevice", "dev1", t)

	writer := newHTTPCommTestResponseWriter()
	request, _ := http.NewRequest(http.MethodGet, "", nil)
	identity := "myorg000/httpDevice/dev1"
	request.SetBasicAuth(identity, "")
	request.Header.Add(security.SPIRequestIdentityHeader, identity)

	httpComm.handleObjects(writer, request)
	if writer.statusCode != http.StatusOK {
		t.Errorf("The call to handleObjects returned %d instead of %d\n", writer.statusCode, http.StatusOK)
	}
	decoder := json.NewDecoder(&writer.body)
	var data []updateMessage
	if err := decoder.Decode(&data); err != nil {
		t.Errorf("Failed to unmarshall meta data. Error: %s\n", err)
	} else {
		if len(data) != readyToSend {
			t.Errorf("For the get updated object test, retrieved %d objects, expected %d objects\n",
				len(data), readyToSend)
		}

		for _, message := range data {
			if message.Type != "update" && message.Type != "delete" {
				t.Errorf("The message for %s:%s wasn't an update or a delete", message.MetaData.ObjectType, message.MetaData.ObjectID)
			}

			if message.Type == "update" {
				writer := newHTTPCommTestResponseWriter()

				dataURL := message.MetaData.DestOrgID + "/" + message.MetaData.ObjectType + "/" +
					message.MetaData.ObjectID + "/" + strconv.FormatInt(message.MetaData.InstanceID, 10) + "/" +
					strconv.FormatInt(message.MetaData.DataID, 10) + "/" + common.Data
				request, _ := http.NewRequest(http.MethodGet, dataURL, nil)
				identity := "myorg000/httpDevice/dev1"
				request.SetBasicAuth(identity, "")
				request.Header.Add(security.SPIRequestIdentityHeader, identity)

				httpComm.handleObjects(writer, request)
				if writer.statusCode != http.StatusOK {
					t.Errorf("The call to handleObjects(data) returned %d instead of %d\n", writer.statusCode, http.StatusOK)
				}
			}

			writer := newHTTPCommTestResponseWriter()
			var operation string
			if message.Type == "update" {
				operation = common.Consumed
			} else {
				operation = common.Deleted
			}
			consumedURL := message.MetaData.DestOrgID + "/" + message.MetaData.ObjectType + "/" +
				message.MetaData.ObjectID + "/" + strconv.FormatInt(message.MetaData.InstanceID, 10) + "/" +
				strconv.FormatInt(message.MetaData.DataID, 10) + "/" + operation
			request, _ := http.NewRequest(http.MethodPut, consumedURL, nil)
			identity := "myorg000/httpDevice/dev1"
			request.SetBasicAuth(identity, "")
			request.Header.Add(security.SPIRequestIdentityHeader, identity)
			httpComm.handleObjects(writer, request)
			if writer.statusCode != http.StatusNoContent {
				t.Errorf("The call to handleObjects(consumed) returned %d instead of %d\n", writer.statusCode, http.StatusNoContent)
			}
		}
	}
}

func TestHttpCommCssMisc(t *testing.T) {
	if status := testHTTPCommSetup("CSS"); status != "" {
		t.Errorf(status)
	}
	defer Store.Stop()
	defer security.Stop()

	destination := common.Destination{DestOrgID: common.Configuration.OrgID, DestType: "httpDevice", DestID: "dev1", Communication: common.HTTPProtocol}
	if err := Store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	identity := common.Configuration.OrgID + "/httpDevice/dev1"

	writer := newHTTPCommTestResponseWriter()
	resendURL := common.Configuration.OrgID + "/" + common.Resend
	request, _ := http.NewRequest(http.MethodPut, resendURL, nil)
	request.SetBasicAuth(identity, "")
	request.Header.Add(security.SPIRequestIdentityHeader, identity)
	httpComm.handleObjects(writer, request)
	if writer.statusCode != http.StatusNoContent {
		t.Errorf("The call to handleObjects(resend) returned %d instead of %d\n", writer.statusCode, http.StatusNoContent)
	}

	// Dummy functions in HTTP communications
	httpComm.SendAckResendObjects(common.Destination{})

	httpComm.ChangeLeadership(true)

	httpComm.Unsubscribe()

	// Some error situations

	writer = newHTTPCommTestResponseWriter()
	resendURL = common.Configuration.OrgID + "/" + common.Resend + "x"
	request, _ = http.NewRequest(http.MethodPut, resendURL, nil)
	request.SetBasicAuth(identity, "")
	request.Header.Add(security.SPIRequestIdentityHeader, identity)
	httpComm.handleObjects(writer, request)
	if writer.statusCode != http.StatusBadRequest {
		t.Errorf("The call to handleObjects(resend) returned %d instead of %d\n", writer.statusCode, http.StatusBadRequest)
	}

	writer = newHTTPCommTestResponseWriter()
	request, _ = http.NewRequest(http.MethodPut, common.Configuration.OrgID, nil)
	request.SetBasicAuth(common.Configuration.OrgID+"/httpDevice@dev1", "")
	request.Header.Add(security.SPIRequestIdentityHeader, identity)

	httpComm.handleRegister(writer, request)
	if writer.statusCode != http.StatusForbidden {
		t.Errorf("The call to handleRegister returned %d instead of %d\n", writer.statusCode, http.StatusForbidden)
	}

	writer = newHTTPCommTestResponseWriter()
	request, _ = http.NewRequest(http.MethodPost, "", nil)
	request.SetBasicAuth(identity, "")
	request.Header.Add(security.SPIRequestIdentityHeader, identity)

	httpComm.handleRegister(writer, request)
	if writer.statusCode != http.StatusMethodNotAllowed {
		t.Errorf("The call to handleRegister returned %d instead of %d\n", writer.statusCode, http.StatusMethodNotAllowed)
	}
}

type httpTestEssSendObjectInfo struct {
	metaData common.MetaData
	action   string
	data     []byte
}

func TestHTTPCommEssSendObjects(t *testing.T) {
	if status := testHTTPCommSetup("CSS"); status != "" {
		t.Errorf(status)
	}
	defer Store.Stop()
	defer security.Stop()

	testObjects := []httpTestEssSendObjectInfo{
		{common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg000", OriginID: "dev1", OriginType: "httpDevice"},
			common.Update, []byte("qazwsxedcrfvtgbyhn")},
		{common.MetaData{ObjectID: "2", ObjectType: "type2", DestOrgID: "myorg000", OriginID: "dev1", OriginType: "httpDevice",
			Deleted: true},
			common.Delete, nil},
	}

	destination := common.Destination{DestOrgID: "myorg000", DestType: "httpDevice", DestID: "dev1", Communication: common.HTTPProtocol}
	if err := Store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	for _, testObject := range testObjects {
		writer := newHTTPCommTestResponseWriter()
		theURL := testObject.metaData.DestOrgID + "/" + testObject.metaData.ObjectType + "/" +
			testObject.metaData.ObjectID + "/1/1/" + testObject.action
		body, err := json.MarshalIndent(testObject.metaData, "", "  ")
		if err != nil {
			t.Errorf("Failed to marshal payload. Error: " + err.Error())
		}
		request, _ := http.NewRequest(http.MethodPut, theURL, bytes.NewReader(body))
		identity := "myorg000/" + testObject.metaData.OriginType + "/" + testObject.metaData.OriginID
		request.SetBasicAuth(identity, "")
		request.Header.Add(security.SPIRequestIdentityHeader, identity)

		httpComm.handleObjects(writer, request)

		if writer.statusCode != http.StatusNoContent && writer.statusCode != http.StatusConflict {
			t.Errorf("The call to handleObjects(%s) returned %d instead of %d\n",
				testObject.action, writer.statusCode, http.StatusNoContent)
		} else {
			if testObject.data != nil {
				theURL = testObject.metaData.DestOrgID + "/" + testObject.metaData.ObjectType + "/" +
					testObject.metaData.ObjectID + "/1/1/" + common.Data
				request, _ := http.NewRequest(http.MethodPut, theURL, bytes.NewReader(testObject.data))
				identity := common.Configuration.OrgID + "/" + testObject.metaData.OriginType + "/" + testObject.metaData.OriginID
				request.SetBasicAuth(identity, "")
				request.Header.Add(security.SPIRequestIdentityHeader, identity)
				httpComm.handleObjects(writer, request)
				if writer.statusCode != http.StatusNoContent {
					t.Errorf("The call to handleObjects(data) returned %d instead of %d\n",
						writer.statusCode, http.StatusNoContent)
				}
			}
		}
	}
}

func TestEssHTTPComm(t *testing.T) {
	defer testEssHTTPCommCleanup()

	time.Sleep(100 * time.Millisecond) // Wait a bit
	security.SetAuthentication(&security.TestAuthenticate{})
	security.Start()
	defer security.Stop()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
	}
	serverPort := listener.Addr().(*net.TCPAddr).Port

	ctx := &testEssCommContext{t: t}

	serverMux := http.NewServeMux()
	serverMux.Handle(registerURL, http.StripPrefix(registerURL, http.HandlerFunc(ctx.testHandleRegister)))
	serverMux.Handle(objectRequestURL, http.StripPrefix(objectRequestURL, http.HandlerFunc(ctx.testHandleObjects)))
	server := http.Server{Handler: serverMux}

	go server.Serve(listener)
	time.Sleep(2 * time.Second)

	defer server.Close()

	common.Configuration.CommunicationProtocol = common.HTTPProtocol
	common.Configuration.HTTPCSSHost = "127.0.0.1"
	common.Configuration.HTTPCSSPort = uint16(serverPort)
	common.HTTPCSSURL = fmt.Sprintf("http://%s:%d", common.Configuration.HTTPCSSHost, common.Configuration.HTTPCSSPort)
	common.Configuration.OrgID = "myorg000"
	common.Configuration.DestinationType = "httpDevice"
	common.Configuration.DestinationID = "dev2"

	if status := testHTTPCommSetup("ESS"); status != "" {
		t.Errorf(status)
	}
	defer Store.Stop()

	ctx.subTest = "register"
	err = httpComm.Register()
	if err != nil {
		t.Error(err)
	}

	ctx.subTest = "poll1"
	if httpComm.Poll() {
		t.Errorf("Poll with no content returned true")
	}

	ctx.pollPayload = []updateMessage{
		{common.Update, common.MetaData{ObjectID: "1", ObjectType: "type2", DestOrgID: "myorg000", NoData: true}},
		{common.Delete, common.MetaData{ObjectID: "2", ObjectType: "type2", DestOrgID: "myorg000", NoData: true}},
		{common.Consumed, common.MetaData{ObjectID: "3", ObjectType: "type2", DestOrgID: "myorg000", NoData: true, InstanceID: 1}},
		{common.Deleted, common.MetaData{ObjectID: "4", ObjectType: "type2", DestOrgID: "myorg000", NoData: true, InstanceID: 1, Deleted: true}},
	}
	statusAfterPoll := []string{common.CompletelyReceived, common.ObjDeleted, common.ConsumedByDest, common.ObjDeleted}

	metaData := ctx.pollPayload[2].MetaData
	Store.StoreObject(metaData, []byte("1234567890abcdefghijkl"), common.ReadyToSend)
	notification := common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.DestID, DestType: metaData.DestType,
		Status: common.Updated, InstanceID: 1}
	Store.UpdateNotificationRecord(notification)

	ctx.subTest = "pushData"
	err = httpComm.pushData(&ctx.pollPayload[2].MetaData)
	if err != nil {
		t.Error(err)
	}

	metaData = ctx.pollPayload[3].MetaData
	Store.StoreObject(metaData, nil, common.ObjDeleted)
	notification = common.Notification{ObjectID: metaData.ObjectID, ObjectType: metaData.ObjectType,
		DestOrgID: metaData.DestOrgID, DestID: metaData.DestID, DestType: metaData.DestType,
		Status: common.ObjDeleted, InstanceID: 1}
	Store.UpdateNotificationRecord(notification)

	ctx.subTest = "poll2"
	if !httpComm.Poll() {
		t.Errorf("Poll with content returned false")
	}

	for index, payload := range ctx.pollPayload {
		metaData := payload.MetaData
		status, _ := Store.RetrieveObjectStatus(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if status != statusAfterPoll[index] {
			t.Errorf("After poll, status=%s, expected=%s\n", status, statusAfterPoll[index])
		}
	}

	ctx.subTest = "getData"
	err = httpComm.GetData(ctx.pollPayload[0].MetaData, 0)
	if err != nil {
		t.Error(err)
	}

	ctx.subTest = "notification"
	notification = common.Notification{ObjectID: "xyzzy", ObjectType: "plover",
		DestOrgID: "myorg000", DestID: "dev2", DestType: "httpDevice",
		Status: common.UpdatePending, InstanceID: 1}

	Store.UpdateNotificationRecord(notification)
	err = httpComm.SendNotificationMessage(common.Update, "httpDevice", "dev2", 1, 1,
		&common.MetaData{ObjectType: "plover", ObjectID: "xyzzy", DestOrgID: "myorg000",
			OriginType: "httpDevice", OriginID: "dev2", InstanceID: 1, DataID: 1})
	if err != nil && !common.IsNotFound(err) {
		t.Error(err)
	}

	err = httpComm.SendNotificationMessage(common.Delete, "httpDevice", "dev2", 1, 1,
		&common.MetaData{ObjectType: "plover", ObjectID: "xyzzy", DestOrgID: "myorg000",
			OriginType: "httpDevice", OriginID: "dev2", InstanceID: 1, DataID: 1})
	if err != nil && !isIgnoredByHandler(err) {
		t.Error(err)
	}

	notification.Status = common.Updated
	err = httpComm.SendNotificationMessage(common.Deleted, "httpDevice", "dev2", 1, 1,
		&common.MetaData{ObjectType: "plover", ObjectID: "xyzzy", DestOrgID: "myorg000",
			OriginType: "httpDevice", OriginID: "dev2", InstanceID: 1, DataID: 1})
	if err != nil && !isIgnoredByHandler(err) {
		t.Error(err)
	}

	notification.Status = common.Updated
	err = httpComm.SendNotificationMessage(common.Consumed, "httpDevice", "dev2", 1, 1,
		&common.MetaData{ObjectType: "plover", ObjectID: "xyzzy", DestOrgID: "myorg000",
			OriginType: "httpDevice", OriginID: "dev2", InstanceID: 1, DataID: 1})
	if err != nil && !isIgnoredByHandler(err) {
		t.Error(err)
	}

	ctx.subTest = "resend"
	err = httpComm.ResendObjects()
	if err != nil {
		t.Error(err)
	}

	// Some error situations

	ctx.subTest = "resendError"
	err = httpComm.ResendObjects()
	if err == nil {
		t.Error("Were suppose to get an error!!")
	}

	ctx.subTest = "registerError"
	err = httpComm.Register()
	if err == nil {
		t.Error("Register Error did NOT return an error")
	}

	if err := Store.DeleteNotificationRecords("", "", "", "", ""); err != nil {
		t.Errorf("Failed to delete notifications. Error: %s", err.Error())
	}
}

func testLoadObjects(testObjects []httpTestObjectInfo, t *testing.T) {
	for _, testObject := range testObjects {
		metaData := testObject.metaData

		// Delete the object first
		if err := storage.DeleteStoredObject(Store, metaData); err != nil {
			t.Errorf("Failed to delete object (objectID = %s). Error: %s\n",
				metaData.ObjectID, err.Error())
		}
		// Insert
		if _, err := Store.StoreObject(metaData, testObject.data, testObject.status); err != nil {
			t.Errorf("Failed to store object (objectID = %s). Error: %s\n", metaData.ObjectID, err.Error())
		}

		Store.DeleteNotificationRecords(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID, "", "")

		if testObject.status == common.NotReadyToSend || metaData.Inactive {
			continue
		}

		// StoreObject increments the instance id, we need to fetch the updated meta data
		updatedMetaData, err := Store.RetrieveObject(metaData.DestOrgID, metaData.ObjectType, metaData.ObjectID)
		if err != nil {
			t.Errorf("Failed to retrieve object (objectID = %s). Error: %s\n",
				metaData.ObjectID, err.Error())
		}

		if metaData.Deleted {
			notificationsInfo, err := PrepareDeleteNotifications(*updatedMetaData)
			if err != nil {
				t.Errorf("Failed to send delete notifications for object (objectID = %s). Error: %s\n",
					metaData.ObjectID, err.Error())
			}
			err = SendNotifications(notificationsInfo)
			if err != nil {
				t.Errorf("Failed to send delete notifications for object (objectID = %s). Error: %s\n",
					metaData.ObjectID, err.Error())
			}
		} else {
			notificationsInfo, err := PrepareObjectNotifications(*updatedMetaData)
			if err != nil {
				t.Errorf("Failed to send notifications for object (objectID = %s). Error: %s\n",
					metaData.ObjectID, err.Error())
			}
			err = SendNotifications(notificationsInfo)
			if err != nil {
				t.Errorf("Failed to send delete notifications for object (objectID = %s). Error: %s\n",
					metaData.ObjectID, err.Error())
			}
		}
	}
}

func registerTestDevice(orgID string, destType string, destID string, t *testing.T) {
	writer := newHTTPCommTestResponseWriter()
	theURL := orgID + "/" + destType + "/" + destID
	request, _ := http.NewRequest(http.MethodPut, theURL, nil)
	request.SetBasicAuth(theURL, "")
	request.Header.Add(security.SPIRequestIdentityHeader, theURL)

	destination := common.Destination{DestOrgID: orgID, DestType: destType, DestID: destID, Communication: common.HTTPProtocol}
	if err := Store.StoreDestination(destination); err != nil {
		t.Errorf("Failed to store destination. Error: %s", err.Error())
	}

	httpComm.handleRegister(writer, request)
	if writer.statusCode != http.StatusNoContent {
		t.Errorf("The call to handleRegister returned %d instead of %d\n", writer.statusCode, http.StatusNoContent)
	}
}

func testEssHTTPCommCleanup() {
	common.Registered = false
}

type testEssCommContext struct {
	t           *testing.T
	subTest     string
	pollPayload []updateMessage
}

func (ctx *testEssCommContext) testHandleRegister(writer http.ResponseWriter, request *http.Request) {
	switch ctx.subTest {
	case "register":

		if request.Method == http.MethodPut {
			parts := strings.Split(request.URL.Path, "/")
			if len(parts) != 3 || parts[0] != common.Configuration.OrgID ||
				parts[1] != common.Configuration.DestinationType || parts[2] != common.Configuration.DestinationID {
				writer.WriteHeader(http.StatusBadRequest)
			} else {
				writer.WriteHeader(http.StatusNoContent)
			}
		} else {
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}

	case "registerError":
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("A dummy error"))
	}
}

func (ctx *testEssCommContext) testHandleObjects(writer http.ResponseWriter, request *http.Request) {
	switch ctx.subTest {
	case "poll1":
		writer.WriteHeader(http.StatusNoContent)

	case "poll2":
		body, err := json.MarshalIndent(ctx.pollPayload, "", "  ")
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.Header().Add("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		writer.Write(body)

	case "getData":
		if request.Method == http.MethodGet {
			writer.Header().Add("Content-Type", "application/octet-stream")
			writer.WriteHeader(http.StatusOK)
			writer.Write([]byte("wsxrfvyhnplijnygv"))
		} else {
			// This is "received" notification sent from httpComm.GetData
			writer.WriteHeader(http.StatusNoContent)
		}

	case "notification":
		writer.WriteHeader(http.StatusNoContent)

	case "pushData":
		writer.WriteHeader(http.StatusNoContent)

	case "resend":
		writer.WriteHeader(http.StatusNoContent)

	case "resendError":
		writer.WriteHeader(http.StatusInternalServerError)

	default:
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
}

func testHTTPCommSetup(nodeType string) string {
	common.Running = true
	time.Sleep(100 * time.Millisecond) // Wait a bit
	common.InitObjectLocks()
	security.SetAuthentication(&security.TestAuthenticate{})
	security.Start()

	if nodeType == common.CSS {
		common.Configuration.MongoDbName = "d_test_db"
		Store = &storage.MongoStorage{}
	} else {
		// Store = &storage.InMemoryStorage{}
		dir, _ := os.Getwd()
		common.Configuration.PersistenceRootPath = dir + "/persist"
		boltStore := &storage.BoltStorage{}
		boltStore.Cleanup()
		Store = boltStore
	}

	if err := Store.Init(); err != nil {
		return fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())
	}

	http.DefaultServeMux = http.NewServeMux()
	httpComm = &HTTP{}
	if err := httpComm.StartCommunication(); err != nil {
		return fmt.Sprintf("Failed to start HTTP communication. Error: %s", err.Error())
	}
	Comm = httpComm

	common.Configuration.NodeType = nodeType
	return ""
}

type httpCommTestResponseWriter struct {
	statusCode int
	header     http.Header
	body       bytes.Buffer
}

func newHTTPCommTestResponseWriter() *httpCommTestResponseWriter {
	writer := new(httpCommTestResponseWriter)
	writer.header = make(map[string][]string)
	return writer
}

func (writer *httpCommTestResponseWriter) Header() http.Header {
	return writer.header
}

func (writer *httpCommTestResponseWriter) Write(p []byte) (int, error) {
	return writer.body.Write(p)
}

func (writer *httpCommTestResponseWriter) WriteHeader(statusCode int) {
	writer.statusCode = statusCode
}
