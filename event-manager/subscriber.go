package eventmanager

import "context"

// Handler processes one record. Nil means its offset is eligible for commit.
type Handler func(context.Context, Message) error

// Middleware wraps a handler.
type Middleware func(Handler) Handler

// Subscriber consumes by topic in a group.
type Subscriber interface {
	Register(string, Handler) error
	Run(context.Context) error
	Ping(context.Context) error
	Close(context.Context) error
}
