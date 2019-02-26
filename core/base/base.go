package base

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-sync-service/core/security"
	"github.com/open-horizon/edge-sync-service/core/storage"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

var store storage.Storage
var communication communications.Communicator

var resendTicker *time.Ticker
var resendStopChannel chan int

var activateTicker *time.Ticker
var activateStopChannel chan int

var waitingOnBlockChannel bool
var blockChannel chan int

var startStopLock sync.Mutex
var started bool

func init() {
	blockChannel = make(chan int, 1)
	resendStopChannel = make(chan int, 1)
	activateStopChannel = make(chan int, 1)
}

// Start starts up the synnc service
func Start(swaggerFile string, registerHandlers bool) common.SyncServiceError {
	startStopLock.Lock()
	defer startStopLock.Unlock()

	if started {
		return &common.SetupError{Message: "An attempt was made to start the Sync Service after it was already started"}
	}

	var ipAddress string
	var err error
	if common.Configuration.ListeningType != common.ListeningUnix {
		ipAddress, err = checkIPAddress(common.Configuration.ListeningAddress)
		if err != nil {
			return err
		}
	} else {
		ipAddress = ""
	}

	if err = setupCertificates(); err != nil {
		return &common.SetupError{Message: fmt.Sprintf("Failed to create TLS certificate. Error: %s\n", err.Error())}
	}

	security.Start()

	if common.Configuration.NodeType == common.CSS {
		mongoStore := &storage.MongoStorage{}
		if common.Configuration.CommunicationProtocol == common.HybridMQTT ||
			common.Configuration.CommunicationProtocol == common.HybridWIoTP {
			store = &storage.Cache{Store: mongoStore}
		} else {
			store = mongoStore
		}
	} else {
		if common.Configuration.ESSPersistentStorage {
			store = &storage.BoltStorage{}
		} else {
			store = &storage.InMemoryStorage{}
		}
	}

	if err := store.Init(); err != nil {
		return &common.SetupError{Message: fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())}
	}
	communications.Store = store
	security.Store = store

	leader.StartLeaderDetermination(store)

	var mqttComm *communications.MQTT
	if (common.Configuration.NodeType == common.ESS && common.Configuration.CommunicationProtocol != common.HTTPProtocol) ||
		(common.Configuration.NodeType == common.CSS && common.Configuration.CommunicationProtocol != common.HTTPProtocol) {
		mqttComm = &communications.MQTT{}
		if err := mqttComm.StartCommunication(); err != nil {
			return &common.SetupError{Message: fmt.Sprintf("Failed to initialize MQTT communication driver. Error: %s\n", err.Error())}
		}
	}

	var httpComm *communications.HTTP
	if (common.Configuration.NodeType == common.ESS && common.Configuration.CommunicationProtocol == common.HTTPProtocol) ||
		(common.Configuration.NodeType == common.CSS &&
			(common.Configuration.CommunicationProtocol == common.HTTPProtocol ||
				common.Configuration.CommunicationProtocol == common.HybridMQTT ||
				common.Configuration.CommunicationProtocol == common.HybridWIoTP)) {
		httpComm = &communications.HTTP{}
		if err := httpComm.StartCommunication(); err != nil {
			return &common.SetupError{Message: fmt.Sprintf("Failed to initialize HTTP communication driver. Error: %s\n", err.Error())}
		}
	}

	communication = communications.NewWrapper(httpComm, mqttComm)
	communications.Comm = communication

	if common.Configuration.NodeType == common.ESS {
		common.Registered = false
		if common.Configuration.CommunicationProtocol == common.HTTPProtocol {
			communication.Register()
		}
	}

	common.ResendAcked = true

	resendTicker = time.NewTicker(time.Second * time.Duration(common.Configuration.ResendInterval))
	go func() {
		keepRunning := true
		for keepRunning {
			select {
			case <-resendTicker.C:
				communications.ResendNotifications()

			case <-resendStopChannel:
				keepRunning = false
			}
		}
		resendTicker = nil
	}()

	activateTicker = time.NewTicker(time.Second * time.Duration(common.Configuration.ObjectActivationInterval))
	go func() {
		keepRunning := true
		for keepRunning {
			select {
			case <-activateTicker.C:
				communications.ActivateObjects()

			case <-activateStopChannel:
				keepRunning = false
			}
		}
		activateTicker = nil
	}()

	err = startHTTPServer(ipAddress, registerHandlers, swaggerFile)
	if err == nil {
		common.Running = true
		started = true
	}
	return err
}

// Stop stops the Sync Server
func Stop(quiesceTime int) {
	startStopLock.Lock()
	defer startStopLock.Unlock()

	common.Running = false

	leader.StopLeadershipPeriodicUpdate()

	if trace.IsLogging(logger.INFO) {
		trace.Info("Stopping in %d seconds", quiesceTime)
	}

	if quiesceTime > 0 {
		timer := time.NewTimer(time.Duration(quiesceTime) * time.Second)
		<-timer.C
	}

	stopHTTPServing()

	communication.StopCommunication()

	security.Stop()

	if resendTicker != nil {
		resendTicker.Stop()
		resendStopChannel <- 1
	}
	if activateTicker != nil {
		activateTicker.Stop()
		activateStopChannel <- 1
	}

	timer := time.NewTimer(time.Duration(2) * time.Second)
	<-timer.C

	store.Stop()

	timer = time.NewTimer(time.Duration(2) * time.Second)
	<-timer.C

	if waitingOnBlockChannel {
		blockChannel <- 1
	}

	started = false
}

// BlockUntilShutdown blocks the current "thread"
func BlockUntilShutdown() {
	waitingOnBlockChannel = true
	_ = <-blockChannel
}

func checkIPAddress(host string) (string, common.SyncServiceError) {

	if host == "" {
		h, err := os.Hostname()
		if err != nil {
			return "", &common.SetupError{Message: fmt.Sprintf("Failed to get hostname. Error: %s\n", err.Error())}
		}
		host = h
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return "", &common.SetupError{Message: fmt.Sprintf("Failed to get IP for the host. Error: %s\n", err.Error())}
	}

	ipAddress := ""
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			ipAddress = ip4.String()
		}
	}

	if ipAddress == "" {
		return "", &common.SetupError{Message: fmt.Sprintf("Failed to get IP for the host.")}
	}
	return ipAddress, nil
}
