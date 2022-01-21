package base

import (
	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

type ObjectWorkQueue struct {
	objectQueue chan common.ObjectInQueue
	bufferSize  uint64
}

func NewObjectWorkQueue(bufferSize uint64) *ObjectWorkQueue {
	q := &ObjectWorkQueue{
		objectQueue: make(chan common.ObjectInQueue, bufferSize),
		bufferSize:  bufferSize,
	}

	go q.run()
	return q
}

func (q *ObjectWorkQueue) run() {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Check object queue to process notifications")
	}
	for {
		select {
		case i, ok := <-q.objectQueue:
			if ok {
				meta := i.Object
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Get an object %s/%s/%s from object Queue, NotificationType: %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.NotificationAction)
				}
				var notificationsInfo []common.NotificationInfo

				switch i.NotificationAction {
				case common.Update:
					if i.NotificationType == common.TypeObject {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Prepare update notifications for %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						notificationsInfo, _ = communications.PrepareObjectNotifications(meta)
					} else if i.NotificationType == common.TypeDestination {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("For object %s/%s/%s, prepare update destination notifications for destinations %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destinations)
						}
						if len(i.Destinations) > 0 {
							notificationsInfo, _ = communications.PrepareNotificationsForDestinations(meta, i.Destinations, common.Update)
						}
					}

				case common.Delete:
					if i.NotificationType == common.TypeObject {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("Prepare delete notifications for %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
						}
						notificationsInfo, _ = communications.PrepareDeleteNotifications(meta)
					} else if i.NotificationType == common.TypeDestination {
						if trace.IsLogging(logger.TRACE) {
							trace.Trace("For object %s/%s/%s, prepare delete destination notifications for destinations %s", meta.DestOrgID, meta.ObjectType, meta.ObjectID, i.Destinations)
						}
						if len(i.Destinations) > 0 {
							notificationsInfo, _ = communications.PrepareNotificationsForDestinations(meta, i.Destinations, common.Delete)
						}
					}
				}

				communications.SendNotifications(notificationsInfo)

				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Sent notifications for %s/%s/%s", meta.DestOrgID, meta.ObjectType, meta.ObjectID)
				}

			} else {
				if trace.IsLogging(logger.TRACE) {
					trace.Trace("Nothing from object Queue")
				}

			}
		}
	}

}

func (q *ObjectWorkQueue) Close() {
	close(q.objectQueue)
}

func (q *ObjectWorkQueue) SendObjectToQueue(objectInQueue common.ObjectInQueue) {
	q.objectQueue <- objectInQueue
}
