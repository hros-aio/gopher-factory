package eventmanager

import "context"

// Publisher sends an event and waits for acknowledgment.
type Publisher interface {
	Publish(context.Context, string, string, Event) error
	Ping(context.Context) error
	Close(context.Context) error
}
