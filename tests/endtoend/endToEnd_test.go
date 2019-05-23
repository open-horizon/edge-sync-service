package endtoend

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"

	"github.com/globalsign/mgo"
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/base"
	"github.com/open-horizon/edge-sync-service/core/security"
)

type syncServiceInfo struct {
	nodeType   string
	id         int
	port       uint16
	command    *exec.Cmd
	context    context.Context
	cancelFunc context.CancelFunc
	stdout     io.Reader
	stderr     io.Reader
}

var cssPort uint16

const (
	mongoDBName             = "d_end_to_end"
	minListeningPort uint16 = 7000
	maxListeningPort uint16 = 7999
)

var currentListeningPort = minListeningPort

var orgIDForTests = "test"
var objectIDForTests = "type1"

var numberOfCSSs int
var numberOfESSs int

func init() {
	flag.IntVar(&numberOfCSSs, "csss", 0, "Number of CSS instances for some tests")
	flag.IntVar(&numberOfESSs, "esss", 0, "Number of ESS instances for some tests")
}

func TestMain(m *testing.M) {

	common.Configuration.MongoDbName = mongoDBName

	err := common.Load("")
	if err != nil {
		fmt.Println("Failed to load configuration")
		os.Exit(1)
	}

	// Don't use common.Configuration.NodeType, it has a default value
	if os.Getenv("NODE_TYPE") == "" {
		os.Exit(m.Run())
	} else {
		runSyncService()
	}
}

func runSyncService() {
	fmt.Printf("Running as %s\n", os.Getenv("NODE_TYPE"))
	base.ConfigStandaloneSyncService()
	base.StandaloneSyncService(&security.TestAuthenticate{})
}

func startSyncService(css bool, storageProvider string, count int, t *testing.T) []syncServiceInfo {
	result := make([]syncServiceInfo, 0, count)

	name, err := os.Executable()
	if err != nil {
		t.Errorf("Failed to get the name of the executable. Error: %s\n", err)
		return nil
	}

	if storageProvider == common.Mongo {
		if err := dropMongoDB(); err != nil {
			t.Errorf("Failed to drop the Mongo DB %s. Error: %s\n", mongoDBName, err)
			return nil
		}
	}

	for index := 0; index < count; index++ {
		context, cancel := context.WithCancel(context.Background())
		command := exec.CommandContext(context, name, "-c", "end-to-end.conf")

		port := getListeningPort()
		if css && index == 0 {
			cssPort = port
		}

		env := make([]string, 0, 100)
		var nodeType string
		if css {
			nodeType = common.CSS
			env = append(env,
				fmt.Sprintf("MONGO_DB_NAME=%s", mongoDBName),
			)
		} else {
			nodeType = common.ESS
			env = append(env,
				"LISTENING_TYPE=unsecure",
				"DESTINATION_TYPE=END-TO-END-TEST",
				fmt.Sprintf("DESTINATION_ID=ess%d", index),
				fmt.Sprintf("OrgID=%s", orgIDForTests),
				"HTTP_CSS_HOST=localhost",
				fmt.Sprintf("HTTP_CSS_PORT=%d", cssPort),
				"HTTP_POLLING_INTERVAL=1",
				"GOMAXPROCS=5",
			)
		}

		lowerNodeType := strings.ToLower(nodeType)

		persistenceRootPath := fmt.Sprintf("./%s%d/persist", lowerNodeType, index)
		if err = os.RemoveAll(persistenceRootPath); err != nil {
			t.Errorf("Failed to remove old persistence directory. Error: %s", err)
		}
		if err = os.MkdirAll(persistenceRootPath, 0700); err != nil {
			t.Errorf("Failed to create persistence directory. Error: %s", err)
		}

		logRootPath := fmt.Sprintf("./%s%d/log", lowerNodeType, index)
		if err = os.RemoveAll(logRootPath); err != nil {
			t.Errorf("Failed to remove old log directory. Error: %s", err)
		}
		if err = os.MkdirAll(logRootPath, 0700); err != nil {
			t.Errorf("Failed to create log directory. Error: %s", err)
		}

		traceRootPath := fmt.Sprintf("./%s%d/trace", lowerNodeType, index)
		if err = os.RemoveAll(traceRootPath); err != nil {
			t.Errorf("Failed to remove old trace directory. Error: %s", err)
		}
		if err = os.MkdirAll(traceRootPath, 0700); err != nil {
			t.Errorf("Failed to create trace directory. Error: %s", err)
		}

		env = append(env,
			fmt.Sprintf("NODE_TYPE=%s", nodeType),
			fmt.Sprintf("STORAGE_PROVIDER=%s", storageProvider),
			"COMMUNICATION_PROTOCOL=http",
			fmt.Sprintf("UNSECURE_LISTENING_PORT=%d", port),
			fmt.Sprintf("PERSISTENCE_ROOT_PATH=%s", persistenceRootPath),
			"OBJECT_ACTIVATION_INTERVAL=1",
			"RESEND_INTERVAL=1",
			"LOG_TRACE_DESTINATION=file",
			fmt.Sprintf("LOG_ROOT_PATH=%s", logRootPath),
			fmt.Sprintf("TRACE_ROOT_PATH=%s", traceRootPath),
			"LOG_LEVEL=TRACE",
			"TRACE_LEVEL=TRACE",
		)

		command.Env = env

		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Errorf("Failed to get pipe of stdout. Error: %s\n", err)
			cancel()
			return result
		}
		stderr, err := command.StderrPipe()
		if err != nil {
			t.Errorf("Failed to get pipe of stderr. Error: %s\n", err)
			cancel()
			return result
		}

		err = command.Start()
		if err != nil {
			t.Errorf("Failed to start %s%d. Error: %s\n", lowerNodeType, index, err)
			cancel()
			return result
		}

		result = append(result, syncServiceInfo{nodeType, index, port, command, context, cancel, stdout, stderr})
	}

	return result
}

func stopSyncService(servers []syncServiceInfo, t *testing.T) {
	for _, server := range servers {
		server.cancelFunc()

		stdoutBytes, err := ioutil.ReadAll(server.stdout)
		if err != nil {
			t.Errorf("Failed to get stdout from %s-%d. Error: %s\n", server.nodeType, server.id, err)
			return
		}
		t.Logf("stdout from %s-%d \n%s\n", server.nodeType, server.id, string(stdoutBytes))

		stderrBytes, err := ioutil.ReadAll(server.stderr)
		if err != nil {
			t.Errorf("Failed to get stderr from %s-%d. Error: %s\n", server.nodeType, server.id, err)
			return
		}
		t.Logf("stderr from %s-%d\n%s\n", server.nodeType, server.id, string(stderrBytes))

		file, err := os.Open(fmt.Sprintf("./%s%d/log/sync-service.log", strings.ToLower(server.nodeType), server.id))
		if err != nil {
			t.Errorf("Failed to open the log from %s-%d, Error: %s\n", server.nodeType, server.id, err)
		}
		bytesRead, err := ioutil.ReadAll(file)
		if err != nil {
			t.Errorf("Failed to read the log from %s-%d, Error: %s\n", server.nodeType, server.id, err)
		}
		t.Logf("Log from %s-%d \n%s\n", server.nodeType, server.id, string(bytesRead))

		file, err = os.Open(fmt.Sprintf("./%s%d/trace/sync-service.log", strings.ToLower(server.nodeType), server.id))
		if err != nil {
			t.Errorf("Failed to open the trace from %s-%d, Error: %s\n", server.nodeType, server.id, err)
		}
		bytesRead, err = ioutil.ReadAll(file)
		if err != nil {
			t.Errorf("Failed to read the trace from %s-%d, Error: %s\n", server.nodeType, server.id, err)
		}
		t.Logf("Trace from %s-%d \n%s\n", server.nodeType, server.id, string(bytesRead))

		server.command.Wait()
	}
}

func dropMongoDB() error {
	dialInfo := &mgo.DialInfo{
		Addrs: strings.Split(common.Configuration.MongoAddressCsv, ","),
	}
	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return err
	}
	db := session.DB(mongoDBName)
	err = db.DropDatabase()
	if err == mgo.ErrNotFound {
		err = nil
	}
	session.Close()

	return err
}

func getListeningPort() uint16 {
	result := currentListeningPort

	currentListeningPort++
	if currentListeningPort > maxListeningPort {
		currentListeningPort = minListeningPort
	}

	return result
}

func getNumberOfCSSs(def int) int {
	if numberOfCSSs != 0 {
		return numberOfCSSs
	}
	return def
}

func getNumberOfESSs(def int) int {
	if numberOfESSs != 0 {
		return numberOfESSs
	}
	return def
}

func setupLogger(t *testing.T) {
	if t != nil {
		parms := logger.Parameters{RootPath: "./" + t.Name(), FileName: "log", Prefix: "", Level: "TRACE",
			MaxFileSize: 409600, MaxCompressedFilesNumber: 2, Destinations: "file", MaintenanceInterval: 3600}
		log.Init(parms)
		parms.FileName = "trace"
		trace.Init(parms)
	} else {
		log.Stop()
		trace.Stop()

		parms := logger.Parameters{Level: "NONE", MaxFileSize: 0, MaxCompressedFilesNumber: 0, Destinations: "stdout", MaintenanceInterval: 0}
		log.Init(parms)
		trace.Init(parms)
	}
}
