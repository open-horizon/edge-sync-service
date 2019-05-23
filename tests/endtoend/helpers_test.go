package endtoend

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/open-horizon/edge-sync-service/common"
)

type driverInfo struct {
	cssInfo         []syncServiceInfo
	essInfo         []syncServiceInfo
	toESS           bool
	receivingClient *client.SyncServiceClient
	sendingClient   *client.SyncServiceClient
	proxy           *proxyInfo
	destType        string
	destID          string
	testCtx         *testContext
	expectedStatus  map[string]string
}

type proxyInfo struct {
	cssPort         uint16
	callsBlocked    bool
	proxyHTTPServer *http.Server
	reverseProxy    *httputil.ReverseProxy
}

func startSyncServiceProxy() *proxyInfo {
	info := proxyInfo{cssPort: cssPort, proxyHTTPServer: &http.Server{}}

	proxyURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d/", info.cssPort))
	info.reverseProxy = httputil.NewSingleHostReverseProxy(proxyURL)

	cssPort = getListeningPort()
	info.proxyHTTPServer.Addr = fmt.Sprintf(":%d", cssPort)

	info.proxyHTTPServer.Handler = &info

	go info.proxyHTTPServer.ListenAndServe()

	time.Sleep(2 * time.Second)

	return &info
}

func (info *proxyInfo) blockCalls(block bool) {
	info.callsBlocked = block
}

func (info *proxyInfo) stop() {
	info.proxyHTTPServer.Shutdown(context.Background())
}

func (info *proxyInfo) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if info.callsBlocked {
		writer.WriteHeader(http.StatusBadGateway)
	} else {
		info.reverseProxy.ServeHTTP(writer, request)
	}
}

type ingressInfo struct {
	ingressHTTPServer *http.Server
	reverseProxies    []*httputil.ReverseProxy
	index             int
	indexLock         *sync.Mutex
}

func startSyncServiceIngress(cssInfo []syncServiceInfo) *ingressInfo {
	info := ingressInfo{ingressHTTPServer: &http.Server{}, indexLock: &sync.Mutex{}}

	cssPort = getListeningPort()
	info.ingressHTTPServer.Addr = fmt.Sprintf(":%d", cssPort)

	info.ingressHTTPServer.Handler = &info

	info.reverseProxies = make([]*httputil.ReverseProxy, len(cssInfo))
	for index, css := range cssInfo {
		proxyURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d/", css.port))
		info.reverseProxies[index] = httputil.NewSingleHostReverseProxy(proxyURL)
	}

	go info.ingressHTTPServer.ListenAndServe()

	time.Sleep(2 * time.Second)

	return &info
}

func (info *ingressInfo) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	info.indexLock.Lock()
	reverseProxy := info.reverseProxies[info.index]
	info.index++
	if info.index >= len(info.reverseProxies) {
		info.index = 0
	}
	info.indexLock.Unlock()

	reverseProxy.ServeHTTP(writer, request)
}

func (info *ingressInfo) stop() {
	info.ingressHTTPServer.Shutdown(context.Background())
}

func newClient(info syncServiceInfo, admin bool) *client.SyncServiceClient {
	syncClient := client.NewSyncServiceClient("http", "localhost", info.port)

	if info.nodeType == common.CSS {
		syncClient.SetOrgID(orgIDForTests)
	}

	var appKey string
	if admin {
		appKey = fmt.Sprintf("testerAdmin@%s", orgIDForTests)
	} else {
		appKey = fmt.Sprintf("testerUser@%s", orgIDForTests)
	}
	syncClient.SetAppKeyAndSecret(appKey, "")

	return syncClient
}

func testUpdateObjectOneTestHelper(test *updateObjectTest, driver *driverInfo, destTest int, objectPerESS bool, essID int, t *testing.T) bool {
	metadata := *test.metadata
	if test.operation == testOpAutoActivate {
		metadata.ActivationTime = time.Now().Add(time.Duration(2) * time.Second).UTC().Format(time.RFC3339)
	} else {
		metadata.ActivationTime = ""
	}
	if test.operation == testOpExpiration {
		metadata.Expiration = time.Now().Add(time.Duration(6) * time.Second).UTC().Format(time.RFC3339)
	} else {
		metadata.Expiration = ""
	}

	if objectPerESS {
		metadata.ObjectID = fmt.Sprintf("%s-ess%d", metadata.ObjectID, essID)
	}

	switch destTest {
	case testDestTypeIDPair:
		metadata.DestType = driver.destType
		if objectPerESS {
			metadata.DestID = fmt.Sprintf("ess%d", essID)
		} else {
			metadata.DestID = driver.destID
		}
		metadata.DestinationsList = nil

	case testDestType:
		metadata.DestType = driver.destType
		metadata.DestID = ""
		metadata.DestinationsList = nil

	case testDestList:
		metadata.DestType = ""
		metadata.DestID = ""
		metadata.DestinationsList = []string{driver.destType + ":" + driver.destID}
	}

	changing := testContextChanging{testID: test.num, consume: test.consume, expectedObject: test.metadata, testMap: driver.testCtx.changing.testMap}
	if !test.metadata.NoData {
		changing.expectedData = []byte(test.data)
	} else {
		changing.expectedData = nil
	}
	driver.testCtx.changeTestData(&changing)

	expectedStatusID := metadata.ObjectType + ":" + metadata.ObjectID

	switch test.operation {
	case testOpUpdate, testOpAutoActivate, testOpExpiration:
		if err := driver.sendingClient.UpdateObject(&metadata); err != nil {
			if driver.toESS || test.operation != testOpExpiration {
				t.Errorf("Failed to update object %s:%s in test number %d. Error: %s",
					metadata.ObjectType, metadata.ObjectID, test.num, err)
			} else {
				return false
			}
		} else {
			if !driver.toESS && test.operation == testOpExpiration {
				t.Error("Update of object to ESS with expiration should have failed")
			}
		}

		if !metadata.NoData && !metadata.MetaOnly {
			if err := driver.sendingClient.UpdateObjectData(&metadata, bytes.NewBuffer([]byte(test.data))); err != nil {
				t.Errorf("Failed to update object data %s:%s in test number %d. Error: %s",
					metadata.ObjectType, metadata.ObjectID, test.num, err)
			}
		}

		if !metadata.Inactive || test.operation == testOpAutoActivate {
			if test.consume {
				driver.expectedStatus[expectedStatusID] = common.Consumed
			} else {
				driver.expectedStatus[expectedStatusID] = common.Delivered
			}
		}

	case testOpDelete:
		driver.testCtx.changing.expectedObject.Deleted = true
		if err := driver.sendingClient.DeleteObject(metadata.ObjectType, metadata.ObjectID); err != nil {
			t.Errorf("Failed to delete object %s:%s in test number %d. Error: %s",
				metadata.ObjectType, metadata.ObjectID, test.num, err)
		}

		delete(driver.expectedStatus, expectedStatusID)

	case testOpActivate:
		if err := driver.sendingClient.ActivateObject(&metadata); err != nil {
			t.Errorf("Failed to activate object %s:%s in test number %d. Error: %s",
				metadata.ObjectType, metadata.ObjectID, test.num, err)
		}

		if test.consume {
			driver.expectedStatus[expectedStatusID] = common.Consumed
		} else {
			driver.expectedStatus[expectedStatusID] = common.Delivered
		}
	}

	return true
}

func setupDriver(toESS bool, cssStorageProvider string, essStorageProvider string, withProxy bool, blocked bool, t *testing.T) *driverInfo {
	driver := driverInfo{toESS: toESS}

	driver.cssInfo = startSyncService(true, cssStorageProvider, 1, t)

	if withProxy {
		driver.proxy = startSyncServiceProxy()
		driver.proxy.blockCalls(blocked)
	}

	driver.essInfo = startSyncService(false, essStorageProvider, 1, t)

	time.Sleep(6 * time.Second)

	cssClient := newClient(driver.cssInfo[0], true)
	essClient := newClient(driver.essInfo[0], true)

	if toESS {
		driver.sendingClient = cssClient
		driver.receivingClient = essClient
		driver.destType = "END-TO-END-TEST"
		driver.destID = fmt.Sprintf("ess%d", driver.essInfo[0].id)
	} else {
		driver.sendingClient = essClient
		driver.receivingClient = cssClient
	}

	driver.testCtx = &testContext{
		objectChannel: make(chan *client.ObjectMetaData, 20),
		stopChannel:   make(chan int, 1),
		resultChannel: make(chan resultChannelData, 20),
		changing:      &testContextChanging{},
	}
	go driver.testCtx.objectUpdateReceiver(0, false, driver.receivingClient)
	driver.receivingClient.StartPollingForUpdates(objectIDForTests, 1, driver.testCtx.objectChannel)

	driver.expectedStatus = make(map[string]string)

	return &driver
}

func checkObjectUpdatesAfterTest(driver *driverInfo, toESS bool, t *testing.T) {
	if toESS {
		if objStatus, err := driver.sendingClient.GetDestinationObjects(driver.destType, driver.destID); err != nil {
			t.Errorf("Failed to retrieve status of objects sent to %s/%s. Error: %s", driver.destType, driver.destID, err)
		} else {

			for _, status := range objStatus {
				if expected, ok := driver.expectedStatus[status.ObjectType+":"+status.ObjectID]; ok {
					if expected != status.Status {
						t.Errorf("The status for %s:%s was %s. Expected %s.", status.ObjectType, status.ObjectID, status.Status, expected)
					}
				} else {
					t.Errorf("Received status for %s:%s that wasn't in the expected set of statuses", status.ObjectType, status.ObjectID)
				}
			}
		}
	}
}

type testContextChanging struct {
	testID         int
	consume        bool
	expectedObject *client.ObjectMetaData
	expectedData   []byte
	testMap        map[string]updateObjectTest
}

type testContext struct {
	objectChannel chan *client.ObjectMetaData
	stopChannel   chan int
	resultChannel chan resultChannelData
	changing      *testContextChanging
}

func (ctx *testContext) objectUpdateReceiver(id int, objectPerEss bool, syncClient *client.SyncServiceClient) {
	keepRunning := true
	message := ""
	for keepRunning {
		select {
		case object := <-ctx.objectChannel:
			consume := ctx.changing.consume
			expectedObject := ctx.changing.expectedObject
			if ctx.changing.testMap != nil {
				if test, ok := ctx.changing.testMap[object.ObjectID]; ok {
					consume = test.consume
					expectedObject = test.metadata
				}
			}

			var expectedObjectID string
			if objectPerEss {
				expectedObjectID = fmt.Sprintf("%s-ess%d", expectedObject.ObjectID, id)
			} else {
				expectedObjectID = expectedObject.ObjectID
			}

			if object.ObjectType != expectedObject.ObjectType || object.ObjectID != expectedObjectID {
				message = fmt.Sprintf("Received the incorrect object. Expected %s:%s. Received %s:%s",
					ctx.changing.expectedObject.ObjectType, ctx.changing.expectedObject.ObjectID, object.ObjectType, object.ObjectID)
			}
			if object.Deleted != expectedObject.Deleted {
				message = fmt.Sprintf("The Deleted flag of %s:%s was incorrect. Expected %t. Received %t",
					object.ObjectType, object.ObjectID, ctx.changing.expectedObject.Deleted, object.Deleted)
			}
			if object.MetaOnly != expectedObject.MetaOnly {
				message = fmt.Sprintf("The MetaOnly flag of %s:%s was incorrect. Expected %t. Received %t",
					object.ObjectType, object.ObjectID, ctx.changing.expectedObject.MetaOnly, object.MetaOnly)
			}
			if object.NoData != expectedObject.NoData {
				message = fmt.Sprintf("The NoData flag of %s:%s was incorrect. Expected %t. Received %t",
					object.ObjectType, object.ObjectID, ctx.changing.expectedObject.NoData, object.NoData)
			}

			if object.Deleted {
				if err := syncClient.MarkObjectDeleted(object); err != nil {
					message = err.Error()
				}
			} else if !object.NoData {
				var buffer bytes.Buffer
				var err error
				if syncClient.FetchObjectData(object, &buffer) {
					if nil != ctx.changing.expectedData && 0 != bytes.Compare(buffer.Bytes(), ctx.changing.expectedData) {
						message = fmt.Sprintf("Data for %s:%s doesn't match expected data",
							object.ObjectType, object.ObjectID)
					} else {
						if consume {
							err = syncClient.MarkObjectConsumed(object)
						} else {
							err = syncClient.MarkObjectReceived(object)
						}
						if err != nil {
							message = err.Error()
						}
					}
				} else {
					message = fmt.Sprintf("Failed to fetch the data of %s:%s.",
						object.ObjectType, object.ObjectID)
				}
			} else {
				var err error
				if consume {
					err = syncClient.MarkObjectConsumed(object)
				} else {
					err = syncClient.MarkObjectReceived(object)
				}
				if err != nil {
					message = err.Error()
				}
			}
			ctx.resultChannel <- resultChannelData{ctx.changing.testID, id, message}

		case <-ctx.stopChannel:
			keepRunning = false
		}
	}
}

func (ctx *testContext) changeTestData(changing *testContextChanging) {
	ctx.changing.testID = changing.testID
	ctx.changing.consume = changing.consume
	ctx.changing.expectedData = changing.expectedData
	ctx.changing.expectedObject = changing.expectedObject
	ctx.changing.testMap = changing.testMap
}
