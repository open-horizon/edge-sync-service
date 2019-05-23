package common

import (
	"time"
)

// Health status values
const (
	Red    = "red"
	Yellow = "yellow"
	Green  = "green"
)

// HealthStatusInfo describes the health status of the sync-service node
// swagger:model
type HealthStatusInfo struct {
	startTime    time.Time
	lockChannel  chan int
	NodeType     string `json:"nodeType"`
	HealthStatus string `json:"healthStatus"`
	UpTime       uint64 `json:"upTime"`
}

// DBHealthStatusInfo describes the health status of the database of the sync-service node
// swagger:model
type DBHealthStatusInfo struct {
	DBStatus                     string `json:"dbStatus"`
	DisconnectedFromDB           bool   `json:"disconnectedFromDB"`
	disconnectFromDBStartTime    time.Time
	LastDisconnectFromDBDuration uint64 `json:"lastDisconnectFromDBDuration,omitempty"`
	DBReadFailures               uint32 `json:"dbReadFailures"`
	DBWriteFailures              uint32 `json:"dbWriteFailures"`
	lastReadWriteErrorTime       time.Time
	TimeSinceLastReadWriteError  uint64 `json:"timeSinceLastReadWriteError,omitempty"`
}

// MQTTHealthStatusInfo describes the health status of the MQTT connection of the sync-service node
// swagger:model
type MQTTHealthStatusInfo struct {
	MQTTConnectionStatus             string `json:"mqttConnectionStatus"`
	SubscribeFailures                uint32 `json:"subscribeFailures"`
	PublishFailures                  uint32 `json:"publishFailures"`
	DisconnectedFromMQTTBroker       bool   `json:"disconnectedFromMQTTBroker"`
	disconnectFromBrokerStartTime    time.Time
	LastDisconnectFromBrokerDuration uint64 `json:"lastDisconnectFromBrokerDuration,omitempty"`
	lastSubscribeErrorTime           time.Time
	TimeSinceLastSubscribeError      uint64 `json:"timeSinceLastSubscribeError,omitempty"`
	lastPublishErrorTime             time.Time
	TimeSinceLastPublishError        uint64 `json:"timeSinceLastPublishError,omitempty"`
}

// UsageInfo describes the usage of the sync-service node
// swagger:model
type UsageInfo struct {
	ClientRequests uint64 `json:"clientRequests"`
	RegisteredESS  uint32 `json:"registeredESS"`
	StoredObjects  uint32 `json:"storedObjects"`
}

// HealthStatus describes the health status of the sync-service node
var HealthStatus HealthStatusInfo

// DBHealth describes the health status of the database of the sync-service node
var DBHealth DBHealthStatusInfo

// MQTTHealth describes the health status of the MQTT connection of the sync-service node
var MQTTHealth MQTTHealthStatusInfo

// HealthUsageInfo describes the usage of the sync-service node
var HealthUsageInfo UsageInfo

func init() {
	DBHealth = DBHealthStatusInfo{DisconnectedFromDB: true}
	MQTTHealth = MQTTHealthStatusInfo{DisconnectedFromMQTTBroker: true}
	HealthUsageInfo = UsageInfo{}
	HealthStatus = HealthStatusInfo{}
	HealthStatus.lockChannel = make(chan int, 1)
	HealthStatus.lockChannel <- 1
}

// NodeStarted is called when the node starts
func (hs *HealthStatusInfo) NodeStarted() {
	hs.lock()
	defer hs.unLock()
	hs.startTime = time.Now()
	hs.NodeType = Configuration.NodeType
}

// SubscribeFailed increments the subscription failures counter
func (hs *HealthStatusInfo) SubscribeFailed() {
	hs.lock()
	defer hs.unLock()
	MQTTHealth.SubscribeFailures++
	MQTTHealth.lastSubscribeErrorTime = time.Now()
}

// PublishFailed increments the publish failures counter
func (hs *HealthStatusInfo) PublishFailed() {
	hs.lock()
	defer hs.unLock()
	MQTTHealth.PublishFailures++
	MQTTHealth.lastPublishErrorTime = time.Now()
}

// DisconnectedFromBroker sets the DisconnectedFromMQTTBroker to true and the disconnectFromBrokerStartTime to current time
func (hs *HealthStatusInfo) DisconnectedFromBroker() {
	hs.lock()
	defer hs.unLock()
	MQTTHealth.DisconnectedFromMQTTBroker = true
	MQTTHealth.disconnectFromBrokerStartTime = time.Now()
	MQTTHealth.LastDisconnectFromBrokerDuration = 0
}

// ReconnectedToBroker sets the DisconnectedFromMQTTBroker to false and calculates LastDisconnectFromBrokerDuration
func (hs *HealthStatusInfo) ReconnectedToBroker() {
	hs.lock()
	defer hs.unLock()
	MQTTHealth.LastDisconnectFromBrokerDuration = hs.GetLastDisconnectFromBrokerDuration()
	MQTTHealth.DisconnectedFromMQTTBroker = false
}

// GetLastDisconnectFromBrokerDuration returns the duration of the last disconnect from the MQTT broker
// In case the node is currently disconnected, LastDisconnectFromBrokerDuration will be 0, and this function
// has to be called to calculated the duration of the current disconnect.
func (hs *HealthStatusInfo) GetLastDisconnectFromBrokerDuration() uint64 {
	if MQTTHealth.disconnectFromBrokerStartTime.IsZero() {
		return 0
	}
	if MQTTHealth.DisconnectedFromMQTTBroker {
		return uint64(time.Since(MQTTHealth.disconnectFromBrokerStartTime) / time.Millisecond)
	}
	return MQTTHealth.LastDisconnectFromBrokerDuration
}

// DisconnectedFromDatabase sets the DisconnectedFromDB to true and the disconnectFromDBStartTime to current time
func (hs *HealthStatusInfo) DisconnectedFromDatabase() {
	hs.lock()
	defer hs.unLock()
	DBHealth.DisconnectedFromDB = true
	DBHealth.disconnectFromDBStartTime = time.Now()
	DBHealth.LastDisconnectFromDBDuration = 0
}

// ReconnectedToDatabase sets the DisconnectedFromDB to false and the disconnectFromBrokerStartTime to current time
func (hs *HealthStatusInfo) ReconnectedToDatabase() {
	hs.lock()
	defer hs.unLock()
	DBHealth.LastDisconnectFromDBDuration = hs.GetLastDisconnectFromDBDuration()
	DBHealth.DisconnectedFromDB = false
}

// GetLastDisconnectFromDBDuration returns the duration of the last disconnect from the database
// In case the node is currently disconnected, LastDisconnectFromDBDuration will be 0, and this function
// has to be called to calculated the duration of the current disconnect.
func (hs *HealthStatusInfo) GetLastDisconnectFromDBDuration() uint64 {
	if DBHealth.disconnectFromDBStartTime.IsZero() {
		return 0
	}
	if DBHealth.DisconnectedFromDB {
		return uint64(time.Since(DBHealth.disconnectFromDBStartTime) / time.Millisecond)
	}
	return DBHealth.LastDisconnectFromDBDuration
}

// DBReadFailed increments the database read failures counter
func (hs *HealthStatusInfo) DBReadFailed() {
	hs.lock()
	defer hs.unLock()
	DBHealth.DBReadFailures++
	DBHealth.lastReadWriteErrorTime = time.Now()
}

// DBWriteFailed increments the database read failures counter
func (hs *HealthStatusInfo) DBWriteFailed() {
	hs.lock()
	defer hs.unLock()
	DBHealth.DBWriteFailures++
	DBHealth.lastReadWriteErrorTime = time.Now()
}

// ClientRequestReceived increments the client requests counter
func (hs *HealthStatusInfo) ClientRequestReceived() {
	hs.lock()
	defer hs.unLock()
	HealthUsageInfo.ClientRequests++
}

// UpdateHealthInfo updates the current health status of the sync service node
func (hs *HealthStatusInfo) UpdateHealthInfo(details bool, registeredESS uint32, storedObjects uint32) {
	hs.lock()
	defer hs.unLock()

	HealthUsageInfo.RegisteredESS = registeredESS
	HealthUsageInfo.StoredObjects = storedObjects

	DBHealth.DBStatus = Green
	timeSinceLastError := uint64(0)
	if DBHealth.DBReadFailures != 0 || DBHealth.DBWriteFailures != 0 {
		timeSinceLastError = uint64(time.Since(DBHealth.lastReadWriteErrorTime).Seconds())
		DBHealth.TimeSinceLastReadWriteError = timeSinceLastError
	}
	if DBHealth.DisconnectedFromDB {
		DBHealth.DBStatus = Red
	} else if DBHealth.DBReadFailures != 0 || DBHealth.DBWriteFailures != 0 {
		if timeSinceLastError < uint64(Configuration.ResendInterval*12) {
			DBHealth.DBStatus = Red
		} else if timeSinceLastError < uint64(Configuration.ResendInterval*60) {
			DBHealth.DBStatus = Yellow
		}
	}

	MQTTHealth.MQTTConnectionStatus = Green
	if Configuration.CommunicationProtocol != HTTPProtocol {
		timeSinceLastSubError := uint64(0)
		if MQTTHealth.SubscribeFailures != 0 {
			timeSinceLastSubError = uint64(time.Since(MQTTHealth.lastSubscribeErrorTime).Seconds())
			MQTTHealth.TimeSinceLastSubscribeError = timeSinceLastSubError
		}
		timeSinceLastPubError := uint64(0)
		if MQTTHealth.PublishFailures != 0 {
			timeSinceLastPubError = uint64(time.Since(MQTTHealth.lastPublishErrorTime).Seconds())
			MQTTHealth.TimeSinceLastPublishError = timeSinceLastPubError
		}
		if MQTTHealth.DisconnectedFromMQTTBroker {
			MQTTHealth.MQTTConnectionStatus = Red
		} else {
			if MQTTHealth.SubscribeFailures != 0 {
				if timeSinceLastSubError < uint64(Configuration.ResendInterval*12) {
					MQTTHealth.MQTTConnectionStatus = Red
				} else if timeSinceLastSubError < uint64(Configuration.ResendInterval*60) {
					MQTTHealth.MQTTConnectionStatus = Yellow
				}
			}
			if MQTTHealth.PublishFailures != 0 && MQTTHealth.MQTTConnectionStatus == Green &&
				timeSinceLastPubError < uint64(Configuration.ResendInterval*12) {
				MQTTHealth.MQTTConnectionStatus = Yellow
			}
		}
	}

	if DBHealth.DBStatus == Red || MQTTHealth.MQTTConnectionStatus == Red {
		hs.HealthStatus = Red
	} else if DBHealth.DBStatus == Yellow || MQTTHealth.MQTTConnectionStatus == Yellow {
		hs.HealthStatus = Yellow
	} else {
		hs.HealthStatus = Green
	}

	hs.UpTime = uint64(time.Since(hs.startTime).Seconds())

	if !details {
		return
	}

	if Configuration.CommunicationProtocol != HTTPProtocol {
		MQTTHealth.LastDisconnectFromBrokerDuration = hs.GetLastDisconnectFromBrokerDuration()
	}
	DBHealth.LastDisconnectFromDBDuration = hs.GetLastDisconnectFromDBDuration()
}

func (hs *HealthStatusInfo) lock() {
	<-hs.lockChannel
}

func (hs *HealthStatusInfo) unLock() {
	hs.lockChannel <- 1
}
