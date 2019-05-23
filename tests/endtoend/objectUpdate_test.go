package endtoend

import (
	"fmt"
	"testing"
	"time"

	"github.com/open-horizon/edge-sync-service-client/client"
	"github.com/open-horizon/edge-sync-service/common"
)

const (
	testOpActivate = iota
	testOpAutoActivate
	testOpDelete
	testOpExpiration
	testOpUpdate
)

const (
	testDestTypeIDPair = iota
	testDestType
	testDestList
	testDestMax // Must be last in the list
)

type updateObjectTest struct {
	num       int
	metadata  *client.ObjectMetaData
	operation int
	data      string
	consume   bool
	extraTime int
}

type resultChannelData struct {
	testID  int
	id      int
	message string
}

var nonBlockingUpdateObjectTests = []updateObjectTest{
	{0, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id1"}, testOpUpdate, "qazwsxedcrfvtgbyhnujm", true, 0},
	{1, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id2"}, testOpUpdate, "poiuytrewqlkjhgfdsa", true, 0},
	{2, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id2"}, testOpDelete, "poiuytrewqlkjhgfdsa", true, 0},
	{3, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id3", Inactive: true}, testOpUpdate, "okmijnuhbygvtcrdxesz", true, 0},
	{4, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id3"}, testOpActivate, "okmijnuhbygvtcrdxesz", true, 0},
	{5, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id4", Inactive: true}, testOpAutoActivate, "zxcvbnmasdfghjkl", true, 4},
	{6, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id5", NoData: true}, testOpUpdate, "", true, 0},
	{7, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id6"}, testOpExpiration, "poiuytrewqlkjhgfdsa", true, 4},
	{8, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id7"}, testOpUpdate, "qwertyuiopasdfghjkl", false, 0},
	{9, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id8"}, testOpUpdate, "rdxtfcygvplokmijnuhb", false, 0},
	{10, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id8"}, testOpDelete, "rdxtfcygvplokmijnuhb", false, 0},
	{11, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id9", Inactive: true}, testOpUpdate, "otfcrdxeszwaqplokmijn", false, 0},
	{12, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id9"}, testOpActivate, "otfcrdxeszwaqplokmijn", false, 0},
	{13, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id10", Inactive: true}, testOpAutoActivate, "asdfghjklzxcvbnm", false, 4},
	{14, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id10", MetaOnly: true}, testOpUpdate, "asdfghjklzxcvbnm", false, 0},
	{15, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id11", NoData: true}, testOpUpdate, "", false, 0},
	{16, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id12"}, testOpExpiration, "poiuytrewqlkjhgfdsa", false, 4},
}

func TestUpdateObjectToCSS(t *testing.T) {
	testUpdateObjectHelper(nonBlockingUpdateObjectTests, false, testDestTypeIDPair, common.Mongo, common.InMemory, t)

	testUpdateObjectHelper(nonBlockingUpdateObjectTests, false, testDestTypeIDPair, common.Bolt, common.Bolt, t)
}

func TestUpdateObjectToESS(t *testing.T) {
	for destTest := 0; destTest < testDestMax; destTest++ {
		testUpdateObjectHelper(nonBlockingUpdateObjectTests, true, destTest, common.Mongo, common.InMemory, t)

		testUpdateObjectHelper(nonBlockingUpdateObjectTests, true, destTest, common.Bolt, common.Bolt, t)
	}
}

var blockedRegistrationUpdateObjectTests = []updateObjectTest{
	{0, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id101"}, testOpUpdate, "qazwsxedcrfvtgbyhnujm", true, 0},
	{1, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id102"}, testOpUpdate, "lkjhgfdsamznxbcvpqowieuryt", false, 0},
	{2, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id103", NoData: true}, testOpUpdate, "", true, 0},
	{3, &client.ObjectMetaData{ObjectType: objectIDForTests, ObjectID: "id104", NoData: true}, testOpUpdate, "", false, 0},
}

func TestBlockedRegistrationUpdateObjectToCSS(t *testing.T) {
	testBlockedRegistrationUpdateObjectHelper(blockedRegistrationUpdateObjectTests, false, testDestTypeIDPair,
		common.Mongo, common.InMemory, t)

	testBlockedRegistrationUpdateObjectHelper(blockedRegistrationUpdateObjectTests, false, testDestTypeIDPair,
		common.Bolt, common.Bolt, t)
}

func TestBlockedRegistrationUpdateObjectToESS(t *testing.T) {
	for destTest := 0; destTest < testDestMax; destTest++ {
		if destTest != testDestList {
			testBlockedRegistrationUpdateObjectHelper(blockedRegistrationUpdateObjectTests, true, destTest, common.Mongo, common.InMemory, t)

			testBlockedRegistrationUpdateObjectHelper(blockedRegistrationUpdateObjectTests, true, destTest, common.Bolt, common.Bolt, t)
		}
	}
}

func TestSendToMultipleESSs(t *testing.T) {
	testSendToMultipleESSsHelper(nonBlockingUpdateObjectTests, 1, false, common.Mongo, common.InMemory, t)
}

func TestSendToMultipleESSsObjectPerESS(t *testing.T) {
	testSendToMultipleESSsHelper(nonBlockingUpdateObjectTests, 1, true, common.Mongo, common.InMemory, t)
}

func TestMultipleCSSsToESSs(t *testing.T) {
	testSendToMultipleESSsHelper(nonBlockingUpdateObjectTests, getNumberOfCSSs(2), false, common.Mongo, common.InMemory, t)
}

func TestMultipleCSSsToESSsObjectPerESS(t *testing.T) {
	testSendToMultipleESSsHelper(nonBlockingUpdateObjectTests, getNumberOfCSSs(2), true, common.Mongo, common.InMemory, t)
}

func testUpdateObjectHelper(tests []updateObjectTest, toESS bool, destTest int,
	cssStorageProvider string, essStorageProvider string, t *testing.T) {

	toESSString := "toCSS"
	if toESS {
		toESSString = "toESS"
	}
	t.Logf("Running TestUpdateObject%s. destination test=%d, cssStorageProvider=%s, essStorageProvider=%s",
		toESSString, destTest, cssStorageProvider, essStorageProvider)

	driver := setupDriver(toESS, cssStorageProvider, essStorageProvider, false, false, t)

	for _, test := range tests {
		if !testUpdateObjectOneTestHelper(&test, driver, destTest, false, 0, t) {
			break
		}

		timer := time.NewTimer(time.Duration(5+test.extraTime) * time.Second)
		select {
		case <-timer.C:
			if !test.metadata.Inactive || test.operation == testOpAutoActivate {
				t.Errorf("Sub test with %s:%s in test number %d timed out.", test.metadata.ObjectType, test.metadata.ObjectID, test.num)
			}

		case data := <-driver.testCtx.resultChannel:
			if test.metadata.Inactive && test.operation != testOpAutoActivate {
				t.Errorf("Received a message for an inactive object %s:%s in test number %d. Message: %s",
					test.metadata.ObjectType, test.metadata.ObjectID, test.num, data.message)
			} else {
				if data.message != "" {
					t.Error(data.message)
				}
			}
			if !timer.Stop() {
				<-timer.C
			}
		}
	}

	driver.testCtx.stopChannel <- 1

	checkObjectUpdatesAfterTest(driver, toESS, t)

	stopSyncService(driver.cssInfo, t)
	stopSyncService(driver.essInfo, t)
}

func testBlockedRegistrationUpdateObjectHelper(tests []updateObjectTest, toESS bool, destTest int,
	cssStorageProvider string, essStorageProvider string, t *testing.T) {

	toESSString := "toCSS"
	if toESS {
		toESSString = "toESS"
	}
	t.Logf("Running TestBlockedRegistrationUpdateObject%s. destination test=%d, cssStorageProvider=%s, essStorageProvider=%s",
		toESSString, destTest, cssStorageProvider, essStorageProvider)

	driver := setupDriver(toESS, cssStorageProvider, essStorageProvider, true, true, t)
	driver.testCtx.changing.testMap = make(map[string]updateObjectTest)

	for _, test := range tests {
		driver.testCtx.changing.testMap[test.metadata.ObjectID] = test
		testUpdateObjectOneTestHelper(&test, driver, destTest, false, 0, t)
	}

	// Draining the channel resultChannel
	timer := time.NewTimer(10 * time.Second)
	keepLooping := true
	for keepLooping {
		select {
		case <-timer.C:
			keepLooping = false

		case data := <-driver.testCtx.resultChannel:
			t.Errorf("Received a message when none were expected. Message: %s", data.message)
		}
	}

	driver.testCtx.changing.expectedData = nil
	driver.proxy.blockCalls(false)
	time.Sleep(5 * time.Second)

	objectsReceivedAtDestination := 0
	keepLooping = true
	timer = time.NewTimer(10 * time.Second)
	for keepLooping {
		select {
		case <-timer.C:
			keepLooping = false

		case data := <-driver.testCtx.resultChannel:
			if data.message != "" {
				t.Error(data.message)
			} else {
				objectsReceivedAtDestination++
			}
		}
	}

	if objectsReceivedAtDestination != len(tests) {
		t.Errorf("The destination did not receive all of the objects. It received %d, expected %d",
			objectsReceivedAtDestination, len(tests))
	}

	checkObjectUpdatesAfterTest(driver, toESS, t)

	driver.testCtx.stopChannel <- 1

	stopSyncService(driver.cssInfo, t)
	stopSyncService(driver.essInfo, t)

	driver.proxy.stop()
}

func testSendToMultipleESSsHelper(tests []updateObjectTest, numOfCSSs int, objectPerESS bool, cssStorageProvider string, essStorageProvider string,
	t *testing.T) {
	numOfESSs := getNumberOfESSs(5)

	driver := &driverInfo{toESS: true}
	driver.destType = "END-TO-END-TEST"
	driver.expectedStatus = make(map[string]string)
	var ingress *ingressInfo

	cssInfo := startSyncService(true, cssStorageProvider, numOfCSSs, t)
	if numOfCSSs > 1 {
		ingress = startSyncServiceIngress(cssInfo)
	}
	essInfo := startSyncService(false, essStorageProvider, numOfESSs, t)

	driver.sendingClient = newClient(syncServiceInfo{nodeType: common.CSS, port: cssPort}, true)
	testContexts := make([]*testContext, 0, len(essInfo))
	resultChannel := make(chan resultChannelData, numOfESSs*5)

	extraTimeForManyESSs := numOfESSs / 25

	for index, ess := range essInfo {
		testCtx := &testContext{
			objectChannel: make(chan *client.ObjectMetaData, 20),
			stopChannel:   make(chan int, 1),
			resultChannel: resultChannel,
			changing:      &testContextChanging{},
		}
		testContexts = append(testContexts, testCtx)
		receivingClient := newClient(ess, true)

		go testCtx.objectUpdateReceiver(index, objectPerESS, receivingClient)
		receivingClient.StartPollingForUpdates("type1", 1, testCtx.objectChannel)
	}
	driver.testCtx = testContexts[0]

	time.Sleep(time.Duration(4+extraTimeForManyESSs) * time.Second)

	for _, test := range tests {
		// Setup extra contexts
		for index := 1; index < len(testContexts); index++ {
			changing := testContextChanging{testID: test.num, consume: test.consume, expectedObject: test.metadata, testMap: driver.testCtx.changing.testMap}
			if !test.metadata.NoData {
				changing.expectedData = []byte(test.data)
			} else {
				changing.expectedData = nil
			}
			testContexts[index].changeTestData(&changing)
		}

		if objectPerESS {
			for essID := 0; essID < len(essInfo); essID++ {
				testUpdateObjectOneTestHelper(&test, driver, testDestTypeIDPair, true, essID, t)
			}
		} else {
			if !testUpdateObjectOneTestHelper(&test, driver, testDestType, false, 0, t) {
				break
			}
		}

		keepWaiting := true
		count := 0
		resultsReceived := make([]bool, len(essInfo), len(essInfo))
		messages := make([]string, 0)
		timer := time.NewTimer(time.Duration(5+extraTimeForManyESSs+test.extraTime) * time.Second)

		for keepWaiting {
			select {
			case <-timer.C:
				keepWaiting = false

			case data := <-resultChannel:
				if data.testID != test.num {
					continue
				}

				if test.metadata.Inactive && test.operation != testOpAutoActivate {
					messages = append(messages,
						fmt.Sprintf("Received a message for an inactive object %s:%s in test number %d. Message: %s",
							test.metadata.ObjectType, test.metadata.ObjectID, test.num, data.message))
				} else {
					if data.message != "" {
						messages = append(messages, fmt.Sprintf("ES %d: %s", data.id, data.message))
					}
				}
				resultsReceived[data.id] = true
				keepWaiting = false
				count++

			checkReceived:
				for _, received := range resultsReceived {
					if !received {
						keepWaiting = true
						break checkReceived
					}
				}
			}
		}

		success := true
		failedESS := -1
		for essID, received := range resultsReceived {
			if !received {
				success = false
				failedESS = essID
				break
			}
		}

		for _, message := range messages {
			t.Error(message)
		}

		if !success && (!test.metadata.Inactive || test.operation == testOpAutoActivate) {
			t.Errorf("Sub test with %s:%s in test number %d on ESS %d timed out.", test.metadata.ObjectType, test.metadata.ObjectID, test.num, failedESS)
		}
	}

	time.Sleep(time.Duration(4+extraTimeForManyESSs) * time.Second)

	for _, testCtx := range testContexts {
		testCtx.stopChannel <- 1
	}

	stopSyncService(essInfo, t)

	if numOfCSSs > 1 {
		ingress.stop()
	}

	stopSyncService(cssInfo, t)
}
