package communications

import (
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type DestinationRequestQueue struct {
	destinationRequestQueue chan common.DestinationRequestInQueue
	bufferSize              uint64
}

func NewDestinationRequestQueue(bufferSize uint64) *DestinationRequestQueue {
	q := &DestinationRequestQueue{
		destinationRequestQueue: make(chan common.DestinationRequestInQueue, bufferSize),
		bufferSize:              bufferSize,
	}

	go q.run()
	return q
}

func (q *DestinationRequestQueue) run() {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Check object queue to process destination update request")
	}

	for {
		select {
		case i, ok := <-q.destinationRequestQueue:
			if ok {
				meta := i.Object
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Get an object %s/%s/%s from destination request queue, update destination %s %s to %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID, i.Status)
				}
				switch i.Action {
				case common.Update:
					requeue := false
					lockIndex := common.HashStrings(meta.DestOrgID, meta.ObjectType, meta.ObjectID)
					common.ObjectLocks.Lock(lockIndex)

					// 1. retrieve notification
					notification, err := Store.RetrieveNotificationRecord(meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
					if err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to retrieve notification for %s %s %s %s %s, resend request to queue", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
						requeue = true
					} else if notification == nil {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Get nil notification for %s %s %s %s %s, ignore destination update request", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
					} else if existingMeta, _, err := Store.RetrieveObjectAndStatus(meta.DestOrgID, meta.ObjectType, meta.ObjectID); err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to retrieve metadata for %s %s %s %s %s, resend request to queue", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
						requeue = true
					} else if existingMeta == nil {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Get nil metadata for %s %s %s %s %s, ignore destination update request", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
					} else if existingMeta.InstanceID != meta.InstanceID {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("ExistingMeta.InstanceID(%d) != meta.InstanceID(%d) for %s %s %s %s %s, ignore destination update request", existingMeta.InstanceID, meta.InstanceID, meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
					} else if notification.Status != common.Data && notification.Status != common.Updated && notification.Status != common.Update && notification.Status != common.UpdatePending && notification.Status != common.ReceivedByDestination {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Notification status (%s) is not one of (data, updated, update, updatePending, receivedByDest) for %s %s %s %s %s, ignore destination update request", notification.Status, meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
					} else if _, err = Store.UpdateObjectDeliveryStatus(i.Status, "", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID); err != nil {
						if log.IsLogging(logger.ERROR) {
							log.Error("Failed to update status to %s of destiantion %s %s %s %s %s, resend request to queue", i.Status, meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
						requeue = true
					} else {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Updated destination status to %s, for %s %s %s %s %s", i.Status, meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
					}

					common.ObjectLocks.Unlock(lockIndex)
					if requeue {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Put request back to queue, for %s %s %s %s %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destination.DestType, i.Destination.DestID)
						}
						q.destinationRequestQueue <- i
					}
				}

			} else {
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Nothing from destination request queue")
				}
			}

		}

	}
}

func (q *DestinationRequestQueue) Close() {
	close(q.destinationRequestQueue)
}

func (q *DestinationRequestQueue) SendDestReqToQueue(destReqInQueue common.DestinationRequestInQueue) {
	q.destinationRequestQueue <- destReqInQueue
}
