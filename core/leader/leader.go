package leader

import (
	"time"

	"github.com/open-horizon/edge-sync-service/core/storage"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"

	"github.com/google/uuid"
)

var leaderID uuid.UUID
var leaderTicker *time.Ticker
var leaderStopChannel chan int
var store storage.Storage
var isLeader bool
var lastTimestamp time.Time

var changeLeadership func(bool) common.SyncServiceError
var unsubscribe func() common.SyncServiceError

func init() {
	leaderID, _ = uuid.NewRandom()
	leaderStopChannel = make(chan int, 1)
}

// StartLeaderDetermination starts the leader determination process
func StartLeaderDetermination(theStore storage.Storage) {
	if common.Configuration.NodeType != common.CSS {
		return
	}
	store = theStore
	isLeader = false

	gotLeadership, err := store.InsertInitialLeader(leaderID.String())

	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error("Failed to insert document into syncLeaderElection collection. Error: %s\n", err)
		}
	}

	if gotLeadership {
		lastTimestamp = time.Now()
		ok, err := store.LeaderPeriodicUpdate(leaderID.String())
		if err != nil && log.IsLogging(logger.ERROR) {
			log.Error("%s\n", err)
		}
		if ok {
			isLeader = true
			if trace.IsLogging(logger.TRACE) {
				trace.Trace("Have taken over as the leader")
			}
		}
	}
	startLeadershipPeriodicUpdate()
}

// CheckIfLeader checks if the current process is the leader
func CheckIfLeader() bool {
	if common.Configuration.NodeType != common.CSS {
		return true
	}
	if !isLeader {
		return false
	}
	if time.Since(lastTimestamp) < time.Second*time.Duration(common.Configuration.LeadershipTimeout) {
		return true
	}

	return false
}

// SetChangeLeaderCallback sets the callback to be called when the leadership changes
func SetChangeLeaderCallback(callback func(bool) common.SyncServiceError) {
	changeLeadership = callback
}

// SetUnsubcribeCallback sets the callback to be called when there is no connection
// to the database and the node has to be unsubscribed from all its subscriptions
func SetUnsubcribeCallback(callback func() common.SyncServiceError) {
	unsubscribe = callback
}

func startLeadershipPeriodicUpdate() {
	leaderTicker = time.NewTicker(time.Second * time.Duration(common.Configuration.LeadershipTimeout) / 5)
	go func() {
		common.GoRoutineStarted()
		keepRunning := true
		for keepRunning {
			select {
			case <-leaderTicker.C:
				if isLeader {
					ok, err := store.LeaderPeriodicUpdate(leaderID.String())
					if err != nil || !ok {
						isLeader = false
						if changeLeadership != nil {
							changeLeadership(false)
						}
						if err != nil {
							if unsubscribe != nil {
								unsubscribe()
							}
							if log.IsLogging(logger.ERROR) {
								log.Error("%s\n", err)
							}
						}
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Have lost the leadership")
						}
					} else {
						lastTimestamp = time.Now()
					}
				} else {
					_, heartbeatTimeout, lastHeartbeatTS, version, err := store.RetrieveLeader()
					if err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("%s\n", err)
						}
					} else {
						timeOnServer, err := store.RetrieveTimeOnServer()
						if err != nil {
							if log.IsLogging(logger.ERROR) {
								log.Error("%s\n", err)
							}
						} else {
							timeSinceHeartBeat := int32(timeOnServer.Sub(lastHeartbeatTS) / time.Second)

							if timeSinceHeartBeat > heartbeatTimeout {
								// Leader seems to have "died", taking over
								updated, err := store.UpdateLeader(leaderID.String(), version)
								if err != nil && log.IsLogging(logger.ERROR) {
									log.Error("%s\n", err)
								}
								if updated {
									if changeLeadership != nil {
										changeLeadership(true)
									}
									isLeader = true
									lastTimestamp = time.Now()
									if trace.IsLogging(logger.TRACE) {
										trace.Trace("Have taken over as the leader")
									}
								}
							}
						}
					}
				}

			case <-leaderStopChannel:
				keepRunning = false
			}
		}
		leaderTicker = nil
		common.GoRoutineEnded()
	}()
}

// StopLeadershipPeriodicUpdate stops the Leadership Periodic Update go routine
func StopLeadershipPeriodicUpdate() {
	if leaderTicker != nil {
		leaderTicker.Stop()
		leaderStopChannel <- 1

		store.ResignLeadership(leaderID.String())
	}
}
