package eventtest

import (
	"context"
	"encoding/json"
	"sync"

	em "github.com/hros-aio/gopher-factory/event-manager"
)

// Published is one acknowledged fake publication.
type Published struct {
	Topic, Key string
	Envelope   em.Envelope[json.RawMessage]
}

// FakePublisher records successful publications in order.
type FakePublisher struct {
	mu                      sync.Mutex
	records                 []Published
	PublishError, PingError error
	closed                  bool
}

// NewFakePublisher creates an empty fake publisher.
func NewFakePublisher() *FakePublisher { return &FakePublisher{} }
func (f *FakePublisher) Publish(ctx context.Context, topic, key string, event em.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return em.ErrClosed
	}
	if f.PublishError != nil {
		return f.PublishError
	}
	e, err := em.PrepareEvent(ctx, event, "eventtest")
	if err != nil {
		return err
	}
	e = *em.Message{Envelope: &e}.Clone().Envelope
	f.records = append(f.records, Published{topic, key, e})
	return nil
}
func (f *FakePublisher) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return em.ErrClosed
	}
	return f.PingError
}
func (f *FakePublisher) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

// Records returns an isolated copy of all acknowledged publications.
func (f *FakePublisher) Records() []Published {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Published, len(f.records))
	copy(out, f.records)
	for i := range out {
		out[i].Envelope = *em.Message{Envelope: &out[i].Envelope}.Clone().Envelope
	}
	return out
}

var _ em.Publisher = (*FakePublisher)(nil)
