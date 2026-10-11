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

func TestRegistration(t *testing.T) {
	c := &Client{cfg: Config{ConsumerEnabled: true, GroupID: "g"}, state: clientState{handlers: make(map[string]em.Handler)}}
	h := func(context.Context, em.Message) error { return nil }
	require.NoError(t, c.Register("sales.created.v1", h))
	assert.ErrorIs(t, c.Register("sales.created.v1", h), em.ErrState)
	assert.ErrorIs(t, c.Register("bad", h), em.ErrInvalidConfig)
	c.state.ran = true
	assert.ErrorIs(t, c.Register("sales.other.v1", h), em.ErrState)
}
func TestRegistrationInvalidModes(t *testing.T) {
	h := func(context.Context, em.Message) error { return nil }
	c := &Client{cfg: Config{DLQTopic: "sales.dead.v1"}, state: clientState{handlers: make(map[string]em.Handler)}}
	assert.ErrorIs(t, c.Register("sales.created.v1", h), em.ErrInvalidConfig)
	c.cfg.ConsumerEnabled = true
	assert.ErrorIs(t, c.Register("sales.created.v1", nil), em.ErrInvalidConfig)
	assert.ErrorIs(t, c.Register("sales.dead.v1", h), em.ErrInvalidConfig)
	assert.ErrorIs(t, c.Run(context.Background()), em.ErrInvalidConfig)
}

type fakeConsumer struct {
	mu        sync.Mutex
	committed []*kgo.Record
	allowed   int
	commitErr error
	polling   chan struct{}
	pollOnce  sync.Once
}

func (f *fakeConsumer) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	if f.polling != nil {
		f.pollOnce.Do(func() { close(f.polling) })
	}
	<-ctx.Done()
	return kgo.Fetches{}
}
func (f *fakeConsumer) CommitRecords(_ context.Context, r ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = append(f.committed, r...)
	return nil
}
func (f *fakeConsumer) AllowRebalance()            { f.mu.Lock(); f.allowed++; f.mu.Unlock() }
func (f *fakeConsumer) CloseAllowingRebalance()    {}
func (f *fakeConsumer) Ping(context.Context) error { return nil }
func testRecord(offset int64) *kgo.Record {
	return &kgo.Record{Topic: "sales.created.v1", Partition: 0, Offset: offset, Value: []byte(`{"eventId":"id","eventType":"created","eventVersion":1,"tenantCode":"","occurredAt":"2026-01-01T00:00:00Z","producer":"node","payload":{}}`)}
}
func testClient(h em.Handler) *Client {
	return &Client{cfg: Config{ClientID: "test", HandlerTimeout: time.Second, PollProcessingTimeout: time.Second, ShutdownTimeout: time.Second, RebalanceTimeout: 12 * time.Second}, state: clientState{handlers: map[string]em.Handler{"sales.created.v1": h}}}
}
func TestBatchCommitFrontier(t *testing.T) {
	defer goleak.VerifyNone(t)
	calls := 0
	c := testClient(func(context.Context, em.Message) error {
		calls++
		if calls == 2 {
			return errors.New("fail")
		}
		return nil
	})
	f := &fakeConsumer{}
	records := []*kgo.Record{testRecord(0), testRecord(1), testRecord(2)}
	err := c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: records}, 3)
	assert.ErrorIs(t, err, em.ErrProcess)
	assert.Equal(t, 2, calls)
	require.Len(t, f.committed, 1)
	assert.Equal(t, int64(0), f.committed[0].Offset)
	assert.Equal(t, 1, f.allowed)
}
func TestBatchCommitFailure(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := testClient(func(context.Context, em.Message) error { return nil })
	f := &fakeConsumer{commitErr: errors.New("broker")}
	err := c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {testRecord(0)}}, 1)
	assert.ErrorIs(t, err, em.ErrCommit)
	assert.Equal(t, 1, f.allowed)
}
func TestBatchMalformedWithoutDLQ(t *testing.T) {
	defer goleak.VerifyNone(t)
	called := false
	c := testClient(func(context.Context, em.Message) error { called = true; return nil })
	f := &fakeConsumer{}
	r := testRecord(0)
	r.Value = []byte("broken")
	err := c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {r}}, 1)
	assert.ErrorIs(t, err, em.ErrProcess)
	assert.False(t, called)
	assert.Empty(t, f.committed)
}
func TestBatchCancellationBounded(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(context.Background())
	c := testClient(func(hctx context.Context, _ em.Message) error { cancel(); <-hctx.Done(); return hctx.Err() })
	c.cfg.ShutdownTimeout = 50 * time.Millisecond
	f := &fakeConsumer{}
	start := time.Now()
	err := c.processBatch(ctx, f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {testRecord(0)}}, 1)
	assert.Error(t, err)
	assert.Less(t, time.Since(start), time.Second)
	assert.Empty(t, f.committed)
}
func TestBatchDeadlineReleasesRebalance(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := testClient(func(ctx context.Context, _ em.Message) error { <-ctx.Done(); return ctx.Err() })
	c.cfg.PollProcessingTimeout = 10 * time.Millisecond
	f := &fakeConsumer{}
	err := c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {testRecord(0)}}, 1)
	assert.Error(t, err)
	assert.Equal(t, 1, f.allowed)
	assert.Empty(t, f.committed)
}
func TestBatchMalformedDLQAcknowledged(t *testing.T) {
	defer goleak.VerifyNone(t)
	called := false
	c := testClient(func(context.Context, em.Message) error { called = true; return nil })
	c.cfg.DLQTopic = "sales.dead.v1"
	p := &fakeProducer{}
	c.producer = p
	c.cfg.PublishTimeout = time.Second
	f := &fakeConsumer{}
	r := testRecord(0)
	r.Value = []byte("malformed")
	require.NoError(t, c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {r}}, 1))
	assert.False(t, called)
	require.Len(t, p.records, 1)
	assert.Equal(t, "sales.dead.v1", p.records[0].Topic)
	require.Len(t, f.committed, 1)
}
func TestBatchPartitionOrderAndParallelism(t *testing.T) {
	defer goleak.VerifyNone(t)
	entered := make(chan int32, 2)
	release := make(chan struct{})
	var mu sync.Mutex
	seen := make(map[int32][]int64)
	c := testClient(func(_ context.Context, m em.Message) error {
		mu.Lock()
		seen[m.Partition] = append(seen[m.Partition], m.Offset)
		mu.Unlock()
		if m.Offset == 0 {
			entered <- m.Partition
			<-release
		}
		return nil
	})
	f := &fakeConsumer{}
	r00, r01, r10 := testRecord(0), testRecord(1), testRecord(0)
	r10.Partition = 1
	done := make(chan error, 1)
	go func() {
		done <- c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {r00, r01}, {"sales.created.v1", 1}: {r10}}, 3)
	}()
	readEntered := func() int32 {
		select {
		case partition := <-entered:
			return partition
		case <-time.After(time.Second):
			close(release)
			t.Fatal("partitions did not run in parallel")
			return -1
		}
	}
	first, second := readEntered(), readEntered()
	assert.NotEqual(t, first, second)
	close(release)
	require.NoError(t, <-done)
	assert.Equal(t, []int64{0, 1}, seen[0])
	assert.Equal(t, []int64{0}, seen[1])
	require.Len(t, f.committed, 2)
}
func TestBatchFailedDLQLeavesOffset(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := testClient(func(context.Context, em.Message) error { return nil })
	c.cfg.DLQTopic = "sales.dead.v1"
	c.cfg.PublishTimeout = time.Second
	c.producer = &fakeProducer{produceErr: errors.New("DLQ unavailable")}
	f := &fakeConsumer{}
	r := testRecord(0)
	r.Value = []byte("malformed")
	err := c.processBatch(context.Background(), f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {r}}, 1)
	assert.ErrorIs(t, err, em.ErrPublish)
	assert.Empty(t, f.committed)
}
func TestBatchNoncooperativeHandlerDoesNotHang(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	c := testClient(func(context.Context, em.Message) error {
		close(entered)
		<-release
		close(exited)
		return nil
	})
	c.cfg.ShutdownTimeout = 40 * time.Millisecond
	f := &fakeConsumer{}
	done := make(chan error, 1)
	go func() {
		done <- c.processBatch(ctx, f, map[partitionKey][]*kgo.Record{{"sales.created.v1", 0}: {testRecord(0)}}, 1)
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, em.ErrShutdownTimeout)
	case <-time.After(time.Second):
		t.Fatal("shutdown hung on non-cooperative handler")
	}
	assert.Empty(t, f.committed)
	close(release)
	<-exited
}
func TestRunCancellationAndClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	p := &fakeProducer{}
	f := &fakeConsumer{polling: make(chan struct{})}
	c := &Client{cfg: Config{Brokers: []string{"fake"}, ClientID: "test", GroupID: "group", ConsumerEnabled: true, ShutdownTimeout: time.Second, MaxPollRecords: 2}, producer: p, opts: options{newConsumer: func(...kgo.Opt) (consumerAPI, error) { return f, nil }}, state: clientState{handlers: map[string]em.Handler{"sales.created.v1": func(context.Context, em.Message) error { return nil }}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	<-f.polling
	cancel()
	require.NoError(t, <-done)
	assert.ErrorIs(t, c.Ping(context.Background()), em.ErrClosed)
	assert.True(t, p.closed)
	assert.ErrorIs(t, c.Run(context.Background()), em.ErrState)
}
