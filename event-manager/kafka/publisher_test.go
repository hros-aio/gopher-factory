package kafka

import (
	"context"
	"errors"
	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

func TestPublisherConfigRejectsInvalid(t *testing.T) {
	_, err := New(context.Background(), Config{})
	assert.ErrorIs(t, err, em.ErrInvalidConfig)
}

type fakeProducer struct {
	mu                            sync.Mutex
	records                       []*kgo.Record
	produceErr, pingErr, flushErr error
	flushBlock                    chan struct{}
	closed                        bool
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rs...)
	return kgo.ProduceResults{{Record: rs[0], Err: f.produceErr}}
}
func (f *fakeProducer) Ping(context.Context) error { return f.pingErr }
func (f *fakeProducer) Flush(context.Context) error {
	if f.flushBlock != nil {
		<-f.flushBlock
	}
	return f.flushErr
}
func (f *fakeProducer) Close() { f.mu.Lock(); f.closed = true; f.mu.Unlock() }
func TestPublishAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	p := &fakeProducer{}
	c := &Client{cfg: Config{ClientID: "worker", PublishTimeout: time.Second, ShutdownTimeout: time.Second}, producer: p}
	e := em.Envelope[int]{EventType: "created", EventVersion: 1, Payload: 7}
	require.NoError(t, c.Publish(context.Background(), "sales.created.v1", "t:a", e))
	require.Len(t, p.records, 1)
	assert.Equal(t, "t:a", string(p.records[0].Key))
	require.NoError(t, c.Close(context.Background()))
	require.NoError(t, c.Close(context.Background()))
	assert.ErrorIs(t, c.Ping(context.Background()), em.ErrClosed)
	assert.ErrorIs(t, c.Publish(context.Background(), "sales.created.v1", "k", e), em.ErrClosed)
	assert.True(t, p.closed)
}
func TestPublishError(t *testing.T) {
	cause := errors.New("ack failed")
	p := &fakeProducer{produceErr: cause}
	c := &Client{cfg: Config{ClientID: "worker", PublishTimeout: time.Second}, producer: p}
	err := c.Publish(context.Background(), "sales.created.v1", "k", em.Envelope[int]{EventType: "created", EventVersion: 1, Payload: 7})
	assert.ErrorIs(t, err, em.ErrPublish)
	assert.ErrorIs(t, err, cause)
}
func TestPublisherHealthAndDeadline(t *testing.T) {
	defer goleak.VerifyNone(t)
	p := &fakeProducer{pingErr: errors.New("offline"), flushBlock: make(chan struct{})}
	c := &Client{cfg: Config{ClientID: "worker", PublishTimeout: time.Second, ShutdownTimeout: time.Second}, producer: p}
	assert.ErrorIs(t, c.Ping(context.Background()), em.ErrPublish)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, c.Publish(ctx, "sales.created.v1", "k", em.Envelope[int]{EventType: "created", EventVersion: 1, Payload: 1}), context.Canceled)
	closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	assert.ErrorIs(t, c.Close(closeCtx), em.ErrShutdownTimeout)
	close(p.flushBlock)
	require.NoError(t, c.Close(context.Background()))
}
