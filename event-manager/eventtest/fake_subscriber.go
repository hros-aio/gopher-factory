package eventtest

import (
	"context"
	"fmt"
	"sync"

	em "github.com/hros-aio/gopher-factory/event-manager"
)

// Delivery captures a deterministic fake handler result and commit eligibility.
type Delivery struct {
	Message   em.Message
	Error     error
	Committed bool
}

// FakeSubscriber calls registered handlers synchronously when Deliver is invoked.
type FakeSubscriber struct {
	mu          sync.Mutex
	handlers    map[string]em.Handler
	deliveries  []Delivery
	ran, closed bool
}

func NewFakeSubscriber() *FakeSubscriber {
	return &FakeSubscriber{handlers: make(map[string]em.Handler)}
}
func (f *FakeSubscriber) Register(topic string, h em.Handler) error {
	if err := em.ValidateTopic(topic); err != nil {
		return err
	}
	if h == nil {
		return fmt.Errorf("%w: nil handler", em.ErrInvalidConfig)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ran || f.closed {
		return em.ErrState
	}
	if _, ok := f.handlers[topic]; ok {
		return em.ErrState
	}
	f.handlers[topic] = h
	return nil
}

// Deliver invokes a registered handler and records whether its offset may commit.
func (f *FakeSubscriber) Deliver(ctx context.Context, m em.Message) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return em.ErrClosed
	}
	h := f.handlers[m.Topic]
	f.mu.Unlock()
	if h == nil {
		return fmt.Errorf("%w: no handler for %s", em.ErrInvalidConfig, m.Topic)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := h(ctx, m.Clone())
	f.mu.Lock()
	f.deliveries = append(f.deliveries, Delivery{m.Clone(), err, err == nil})
	f.mu.Unlock()
	return err
}
func (f *FakeSubscriber) Deliveries() []Delivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Delivery, len(f.deliveries))
	for i, d := range f.deliveries {
		out[i] = d
		out[i].Message = d.Message.Clone()
	}
	return out
}
func (f *FakeSubscriber) Run(ctx context.Context) error {
	f.mu.Lock()
	if f.ran || f.closed {
		f.mu.Unlock()
		return em.ErrState
	}
	f.ran = true
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}
func (f *FakeSubscriber) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return em.ErrClosed
	}
	return nil
}
func (f *FakeSubscriber) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

var _ em.Subscriber = (*FakeSubscriber)(nil)
