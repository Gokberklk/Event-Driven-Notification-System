package notification

import (
	"time"

	"github.com/Gokberklk/Event-Driven-Notification-System/internal/events"
)

// StatusEvent builds a status-change event for n with the given status.
func StatusEvent(n Notification, status Status, errMsg string) events.Event {
	e := events.Event{
		NotificationID: n.ID.String(),
		Channel:        string(n.Channel),
		Status:         string(status),
		Attempts:       n.Attempts,
		Error:          errMsg,
		Timestamp:      time.Now().UTC(),
	}
	if n.BatchID != nil {
		e.BatchID = n.BatchID.String()
	}
	if n.CorrelationID != nil {
		e.CorrelationID = *n.CorrelationID
	}
	return e
}
