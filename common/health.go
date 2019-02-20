package common

import (
	"fmt"
	"time"

	"github.com/open-horizon/edge-utilities/logger/log"
)

// HealthStatusInfo describes the health status of the sync-service node
type HealthStatusInfo struct {
	SubscribeFailures                uint32 `json:"subscribeFailures"`
	PublishFailures                  uint32 `json:"publishFailures"`
	DisconnectedFromMQTTBroker       bool   `json:"disconnectedFromMQTTBroker"`
	disconnectFromBrokerStartTime    time.Time
	LastDisconnectFromBrokerDuration uint64 `json:"lastDisconnectFromBrokerDuration"`
	DisconnectedFromDB               bool   `json:"disconnectedFromDB"`
	disconnectFromDBStartTime        time.Time
	LastDisconnectFromDBDuration     uint64 `json:"lastDisconnectFromDBDuration"`
	DBReadFailures                   uint32 `json:"dbReadFailures"`
	DBWriteFailures                  uint32 `json:"dbWriteFailures"`
	lockChannel                      chan int
}

// HealthStatus describes the health status of the sync-service node
var HealthStatus HealthStatusInfo

func init() {
	HealthStatus = HealthStatusInfo{DisconnectedFromMQTTBroker: true, DisconnectedFromDB: true}
	HealthStatus.lockChannel = make(chan int, 1)
	HealthStatus.lockChannel <- 1
}

// SubscribeFailed increments the subscription failures counter
func (hs *HealthStatusInfo) SubscribeFailed() {
	hs.lock()
	defer hs.unLock()
	hs.SubscribeFailures++
}

// PublishFailed increments the publish failures counter
func (hs *HealthStatusInfo) PublishFailed() {
	hs.lock()
	defer hs.unLock()
	hs.PublishFailures++
}

// DisconnectedFromBroker sets the DisconnectedFromMQTTBroker to true and the disconnectFromBrokerStartTime to current time
func (hs *HealthStatusInfo) DisconnectedFromBroker() {
	hs.lock()
	defer hs.unLock()
	hs.DisconnectedFromMQTTBroker = true
	hs.disconnectFromBrokerStartTime = time.Now()
	hs.LastDisconnectFromBrokerDuration = 0
}

// ReconnectedToBroker sets the DisconnectedFromMQTTBroker to false and calculates LastDisconnectFromBrokerDuration
func (hs *HealthStatusInfo) ReconnectedToBroker() {
	hs.lock()
	defer hs.unLock()
	hs.LastDisconnectFromBrokerDuration = hs.GetLastDisconnectFromBrokerDuration()
	hs.DisconnectedFromMQTTBroker = false
}

// GetLastDisconnectFromBrokerDuration returns the duration of the last disconnect from the MQTT broker
// In case the node is currently disconnected, LastDisconnectFromBrokerDuration will be 0, and this function
// has to be called to calculated the duration of the current disconnect.
func (hs *HealthStatusInfo) GetLastDisconnectFromBrokerDuration() uint64 {
	if hs.disconnectFromBrokerStartTime.IsZero() {
		return 0
	}
	if hs.DisconnectedFromMQTTBroker {
		return uint64(time.Since(hs.disconnectFromBrokerStartTime) / time.Millisecond)
	}
	return hs.LastDisconnectFromBrokerDuration
}

// DisconnectedFromDatabase sets the DisconnectedFromDB to true and the disconnectFromDBStartTime to current time
func (hs *HealthStatusInfo) DisconnectedFromDatabase() {
	hs.lock()
	defer hs.unLock()
	hs.DisconnectedFromDB = true
	hs.disconnectFromDBStartTime = time.Now()
	hs.LastDisconnectFromDBDuration = 0
}

// ReconnectedToDatabase sets the DisconnectedFromDB to false and the disconnectFromBrokerStartTime to current time
func (hs *HealthStatusInfo) ReconnectedToDatabase() {
	hs.lock()
	defer hs.unLock()
	hs.LastDisconnectFromDBDuration = hs.GetLastDisconnectFromDBDuration()
	hs.DisconnectedFromDB = false
}

// GetLastDisconnectFromDBDuration returns the duration of the last disconnect from the database
// In case the node is currently disconnected, LastDisconnectFromDBDuration will be 0, and this function
// has to be called to calculated the duration of the current disconnect.
func (hs *HealthStatusInfo) GetLastDisconnectFromDBDuration() uint64 {
	if hs.disconnectFromDBStartTime.IsZero() {
		return 0
	}
	if hs.DisconnectedFromDB {
		return uint64(time.Since(hs.disconnectFromDBStartTime) / time.Millisecond)
	}
	return hs.LastDisconnectFromDBDuration
}

// DBReadFailed increments the database read failures counter
func (hs *HealthStatusInfo) DBReadFailed() {
	hs.lock()
	defer hs.unLock()
	hs.DBReadFailures++
}

// DBWriteFailed increments the database read failures counter
func (hs *HealthStatusInfo) DBWriteFailed() {
	hs.lock()
	defer hs.unLock()
	hs.DBWriteFailures++
}

// PrintInfo dumps the current status to stdout
func (hs *HealthStatusInfo) PrintInfo() {
	result := fmt.Sprintln("*** Health Status ***")
	result += fmt.Sprintf("* Number of MQTT subscribe failures: %d\n", hs.SubscribeFailures)
	result += fmt.Sprintf("* Number of MQTT publish failures: %d\n", hs.PublishFailures)
	if hs.DisconnectedFromMQTTBroker {
		result += fmt.Sprintf("* Disconnected from MQTT broker")
		if t := hs.GetLastDisconnectFromBrokerDuration(); t != 0 {
			result += fmt.Sprintf(" for %d milliseconds\n", t)
		} else {
			result += fmt.Sprintf("\n")
		}
	} else {
		result += fmt.Sprintln("* Connected to MQTT broker")
		if hs.LastDisconnectFromBrokerDuration != 0 {
			result += fmt.Sprintf("* Last disconnect took %d milliseconds\n", hs.LastDisconnectFromBrokerDuration)
		}
	}

	result += fmt.Sprintf("* Number of the database read failures: %d\n", hs.DBReadFailures)
	result += fmt.Sprintf("* Number of the database write failures: %d\n", hs.DBWriteFailures)
	if hs.DisconnectedFromDB {
		result += fmt.Sprintf("* Disconnected from the database")
		if t := hs.GetLastDisconnectFromDBDuration(); t != 0 {
			result += fmt.Sprintf(" for %d milliseconds\n", t)
		} else {
			result += fmt.Sprintf("\n")
		}
	} else {
		result += fmt.Sprintln("* Connected to the database")
		if hs.LastDisconnectFromDBDuration != 0 {
			result += fmt.Sprintf("* Last disconnect took %d milliseconds\n", hs.LastDisconnectFromDBDuration)
		}
	}
	log.Info(result)
}

func (hs *HealthStatusInfo) lock() {
	<-hs.lockChannel
}

func (hs *HealthStatusInfo) unLock() {
	hs.lockChannel <- 1
}
