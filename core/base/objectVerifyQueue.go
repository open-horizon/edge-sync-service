package base

import (
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/dataVerifier"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type ObjectVerifyQueue struct {
	objectDataVerifyQueue chan common.ObjectInVerifyQueue
	bufferSize            uint64
}

// Only support doing data verification at sender side
func NewObjectVerifyQueue(bufferSize uint64) *ObjectVerifyQueue {
	q := &ObjectVerifyQueue{
		objectDataVerifyQueue: make(chan common.ObjectInVerifyQueue, bufferSize),
		bufferSize:            bufferSize,
	}

	go q.run()
	return q
}

func (q *ObjectVerifyQueue) run() {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Check object verify queue to process object data verification")
	}

	for {
		select {
		case i, ok := <-q.objectDataVerifyQueue:
			if ok {
				meta := i.Object
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Get an object %s/%s/%s from object verification Queue", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
				}

				if common.NeedDataVerification(meta) {
					if trace.IsLogging(logger.TRACE) {
						trace.Trace("Start data verification for %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					}

					lockIndex := common.HashStrings(meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					common.ObjectLocks.Lock(lockIndex)

					// Set object status from "notReady" to "verifying"
					if err := store.UpdateObjectStatus(meta.DestOrgID, meta.ObjectType, meta.ObjectID, common.Verifying); err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to update object status to %s for object %s/%s/%s", common.Verifying, meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}
					status := common.VerificationFailed
					dataVf := dataVerifier.NewDataVerifier(meta.HashAlgorithm, meta.PublicKey, meta.Signature)
					if dr, err := dataVf.GetTempData(meta); err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to get temp data for data verify for object %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						// Set object status from "verifying" to "verificationFailed"
					} else if success, err := dataVf.VerifyDataSignature(dr, meta.DestOrgID, meta.ObjectType, meta.ObjectID, ""); !success || err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to verify data for object %s/%s/%s, remove unverified data", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
							if err != nil {
								log.Error("Error: %s", err.Error())
							}
						}
						dataVf.RemoveUnverifiedData(meta)
						// Set object status from "verifying" to "verification_failed"
					} else {
						status = common.ReadyToSend
					}

					// Data is verified, object status is set to "ready" during VerifyDataSignature(StoreObjectData)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Data verification is done for object %s/%s/%s, updating object status to %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, status)
					}

					if err := store.UpdateObjectStatus(meta.DestOrgID, meta.ObjectType, meta.ObjectID, status); err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to update object status to %s for object %s/%s/%s", status, meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}

					if status == common.VerificationFailed {
						// Verification failed, will return without sending out notificaitons to destinations
						if log.IsLogging(logger.ERROR) {
							log.Error("Verification failed for object %s/%s/%s, return", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}

					// Data is verified, object status is set to "ready" during VerifyDataSignature(StoreObjectData)
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Data verified for object %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					}

					// StoreObject increments the instance id if this is a data update, we need to fetch the updated meta data
					// Also, StoreObjectData updates the ObjectSize, so we need to fetch the updated meta data
					updatedMetaData, err := store.RetrieveObject(meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					if err != nil {
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}

					if updatedMetaData.Inactive {
						// Don't send inactive objects to the other side
						common.ObjectLocks.Unlock(lockIndex)
						continue
					}
					common.ObjectLocks.Unlock(lockIndex)

					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Updating object status to ready for %s/%s/%s\n", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					}

					// Should be in antoher thread
					if trace.IsLogging(logger.DEBUG) {
						trace.Debug("Send object to objectQueue %s/%s/%s\n", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					}

					objectInQueue := common.ObjectInQueue{NotificationAction: common.Update, NotificationType: common.TypeObject, Object: *updatedMetaData, Destinations: []common.StoreDestinationStatus{}}
					objectQueue.SendObjectToQueue(objectInQueue)

				}
			} else {
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Nothing from object verify Queue")
				}

			}
		}
	}
}

func (q *ObjectVerifyQueue) Close() {
	close(q.objectDataVerifyQueue)
}

func (q *ObjectVerifyQueue) SendObjectToQueue(objectInVerifyQueue common.ObjectInVerifyQueue) {
	q.objectDataVerifyQueue <- objectInVerifyQueue
}
