//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/hros-aio/gopher-factory/event-manager/eventtest"
	adapter "github.com/hros-aio/gopher-factory/event-manager/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	containerskafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func broker(t *testing.T) ([]string, *kgo.Client) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	container, err := containerskafka.Run(ctx, "confluentinc/confluent-local:7.5.0", containerskafka.WithClusterID("eventManagerTest"))
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	return brokers, admin
}
func createTopic(t *testing.T, admin *kgo.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	resp, err := req.RequestWith(ctx, admin)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Zero(t, resp.Topics[0].ErrorCode)
}
func cfg(brokers []string, group string) adapter.Config {
	return adapter.Config{Brokers: brokers, ClientID: "event-manager-test", GroupID: group, ConsumerEnabled: group != "", ShutdownTimeout: 5 * time.Second, HandlerTimeout: 3 * time.Second, PollProcessingTimeout: 12 * time.Second, RebalanceTimeout: 25 * time.Second}
}
func published(t *testing.T, p *adapter.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, p.Publish(ctx, topic, "tenant:agg", em.Envelope[map[string]int]{EventType: "created", EventVersion: 1, TenantCode: "tenant", Payload: map[string]int{"amount": 7}}))
}

func TestPublishAndConsumeAcknowledged(t *testing.T) {
	brokers, admin := broker(t)
	topic := "billing.created.v1"
	createTopic(t, admin, topic)
	ctx := context.Background()
	pub, err := adapter.New(ctx, cfg(brokers, ""))
	require.NoError(t, err)
	defer pub.Close(ctx)
	published(t, pub, topic)
	sub, err := adapter.New(ctx, cfg(brokers, "publish-group"))
	require.NoError(t, err)
	got := make(chan em.Message, 1)
	require.NoError(t, sub.Register(topic, func(_ context.Context, m em.Message) error { got <- m; return nil }))
	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(runCtx) }()
	select {
	case m := <-got:
		assert.Equal(t, "tenant:agg", string(m.Key))
		require.NotNil(t, m.Envelope)
		assert.Equal(t, "created", m.Envelope.EventType)
	case <-runCtx.Done():
		t.Fatal("record not consumed")
	}
	cancel()
	require.NoError(t, <-done)
	published(t, pub, topic)
	second, err := adapter.New(context.Background(), cfg(brokers, "publish-group"))
	require.NoError(t, err)
	next := make(chan em.Message, 1)
	require.NoError(t, second.Register(topic, func(_ context.Context, m em.Message) error { next <- m; return nil }))
	secondCtx, stopSecond := context.WithTimeout(context.Background(), 20*time.Second)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	select {
	case m := <-next:
		assert.Equal(t, int64(1), m.Offset)
	case <-secondCtx.Done():
		t.Fatal("second record not consumed")
	}
	stopSecond()
	require.NoError(t, <-secondDone)
}

func TestFailedHandlerRedelivers(t *testing.T) {
	brokers, admin := broker(t)
	topic := "jobs.started.v1"
	createTopic(t, admin, topic)
	pub, err := adapter.New(context.Background(), cfg(brokers, ""))
	require.NoError(t, err)
	defer pub.Close(context.Background())
	published(t, pub, topic)
	group := fmt.Sprintf("redelivery-%d", time.Now().UnixNano())
	first, err := adapter.New(context.Background(), cfg(brokers, group))
	require.NoError(t, err)
	require.NoError(t, first.Register(topic, func(context.Context, em.Message) error { return errors.New("business failure") }))
	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	assert.ErrorIs(t, first.Run(runCtx), em.ErrProcess)
	second, err := adapter.New(context.Background(), cfg(brokers, group))
	require.NoError(t, err)
	got := make(chan em.Message, 1)
	require.NoError(t, second.Register(topic, func(_ context.Context, m em.Message) error { got <- m; return nil }))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	done := make(chan error, 1)
	go func() { done <- second.Run(ctx2) }()
	select {
	case m := <-got:
		assert.Equal(t, int64(0), m.Offset)
	case <-ctx2.Done():
		t.Fatal("failed record not redelivered")
	}
	cancel2()
	require.NoError(t, <-done)
}

func TestPermanentFailureGoesToDLQ(t *testing.T) {
	brokers, admin := broker(t)
	topic, dlq := "orders.failed.v1", "orders.dead.v1"
	createTopic(t, admin, topic)
	createTopic(t, admin, dlq)
	base := cfg(brokers, "")
	pub, err := adapter.New(context.Background(), base)
	require.NoError(t, err)
	defer pub.Close(context.Background())
	published(t, pub, topic)
	c := cfg(brokers, fmt.Sprintf("dlq-%d", time.Now().UnixNano()))
	c.DLQTopic = dlq
	sub, err := adapter.New(context.Background(), c)
	require.NoError(t, err)
	handled := make(chan struct{}, 1)
	h := em.Chain(func(context.Context, em.Message) error {
		handled <- struct{}{}
		return em.MarkPermanent(errors.New("invalid order"))
	}, em.DeadLetter(pub, dlq), em.Retry(em.RetryConfig{}), em.Recover())
	require.NoError(t, sub.Register(topic, h))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()
	select {
	case <-handled:
	case <-ctx.Done():
		t.Fatal("source not handled")
	}
	cancel()
	require.NoError(t, <-done)
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(dlq))
	require.NoError(t, err)
	defer reader.Close()
	readCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	records := reader.PollRecords(readCtx, 1)
	n := 0
	records.EachRecord(func(r *kgo.Record) { n++; assert.Equal(t, dlq, r.Topic) })
	require.Equal(t, 1, n)
}

func TestMalformedRecordGoesToDLQ(t *testing.T) {
	brokers, admin := broker(t)
	topic, dlq := "events.input.v1", "events.dead.v1"
	createTopic(t, admin, topic)
	createTopic(t, admin, dlq)
	_, err := admin.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: []byte("key"), Value: []byte("not-json")}).First()
	require.NoError(t, err)
	c := cfg(brokers, fmt.Sprintf("malformed-%d", time.Now().UnixNano()))
	c.DLQTopic = dlq
	sub, err := adapter.New(context.Background(), c)
	require.NoError(t, err)
	called := make(chan struct{}, 1)
	require.NoError(t, sub.Register(topic, func(context.Context, em.Message) error { called <- struct{}{}; return nil }))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(dlq))
	require.NoError(t, err)
	defer reader.Close()
	var payload em.DeadLetterData
	reader.PollRecords(ctx, 1).EachRecord(func(r *kgo.Record) {
		var envelope em.Envelope[json.RawMessage]
		require.NoError(t, json.Unmarshal(r.Value, &envelope))
		require.NoError(t, json.Unmarshal(envelope.Payload, &payload))
	})
	require.Equal(t, []byte("not-json"), payload.RawValue)
	assert.Equal(t, "invalid_event", payload.FailureClass)
	assert.Equal(t, "invalid event envelope", payload.FailureMessage)
	assert.Empty(t, called)
	cancel()
	require.NoError(t, <-done)
}

func TestRetryExhaustionGoesToDLQ(t *testing.T) {
	brokers, admin := broker(t)
	topic, dlq := "jobs.retry.v1", "jobs.dead.v1"
	createTopic(t, admin, topic)
	createTopic(t, admin, dlq)
	pub, err := adapter.New(context.Background(), cfg(brokers, ""))
	require.NoError(t, err)
	defer pub.Close(context.Background())
	published(t, pub, topic)
	c := cfg(brokers, fmt.Sprintf("retry-%d", time.Now().UnixNano()))
	c.DLQTopic = dlq
	sub, err := adapter.New(context.Background(), c)
	require.NoError(t, err)
	var attempts atomic.Int32
	h := em.Chain(func(context.Context, em.Message) error {
		attempts.Add(1)
		return em.MarkRetryable(errors.New("transient"))
	}, em.DeadLetter(pub, dlq), em.Retry(em.RetryConfig{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}), em.Recover())
	require.NoError(t, sub.Register(topic, h))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sub.Run(ctx) }()
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(dlq))
	require.NoError(t, err)
	defer reader.Close()
	var payload em.DeadLetterData
	reader.PollRecords(ctx, 1).EachRecord(func(r *kgo.Record) {
		var envelope em.Envelope[json.RawMessage]
		require.NoError(t, json.Unmarshal(r.Value, &envelope))
		require.NoError(t, json.Unmarshal(envelope.Payload, &payload))
	})
	require.Equal(t, 2, payload.Attempts)
	assert.Equal(t, int32(2), attempts.Load())
	cancel()
	require.NoError(t, <-done)
}

func TestGroupHandoffDoesNotOverlapCooperativeHandler(t *testing.T) {
	brokers, admin := broker(t)
	topic := "handoff.started.v1"
	createTopic(t, admin, topic)
	pub, err := adapter.New(context.Background(), cfg(brokers, ""))
	require.NoError(t, err)
	defer pub.Close(context.Background())
	published(t, pub, topic)
	group := fmt.Sprintf("handoff-%d", time.Now().UnixNano())
	firstCfg := cfg(brokers, group)
	firstCfg.ShutdownTimeout = time.Second
	first, err := adapter.New(context.Background(), firstCfg)
	require.NoError(t, err)
	entered, exited := make(chan struct{}), make(chan struct{})
	require.NoError(t, first.Register(topic, func(ctx context.Context, _ em.Message) error {
		close(entered)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}))
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()
	select {
	case <-entered:
	case <-firstCtx.Done():
		t.Fatal("first group member did not receive record")
	}
	second, err := adapter.New(context.Background(), cfg(brokers, group))
	require.NoError(t, err)
	overlap := make(chan struct{}, 1)
	got := make(chan em.Message, 1)
	require.NoError(t, second.Register(topic, func(_ context.Context, m em.Message) error {
		select {
		case <-exited:
		default:
			overlap <- struct{}{}
		}
		got <- m
		return nil
	}))
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	cancelFirst()
	assert.Error(t, <-firstDone)
	select {
	case m := <-got:
		assert.Equal(t, int64(0), m.Offset)
	case <-secondCtx.Done():
		t.Fatal("second member did not receive redelivery")
	}
	assert.Empty(t, overlap)
	cancelSecond()
	require.NoError(t, <-secondDone)
}

func TestFailedDLQHandoffLeavesSourceOffset(t *testing.T) {
	brokers, admin := broker(t)
	topic := "handoff.failed.v1"
	createTopic(t, admin, topic)
	pub, err := adapter.New(context.Background(), cfg(brokers, ""))
	require.NoError(t, err)
	defer pub.Close(context.Background())
	published(t, pub, topic)
	group := fmt.Sprintf("failed-dlq-%d", time.Now().UnixNano())
	first, err := adapter.New(context.Background(), cfg(brokers, group))
	require.NoError(t, err)
	failingDLQ := eventtest.NewFakePublisher()
	failingDLQ.PublishError = errors.New("DLQ broker unavailable")
	h := em.Chain(func(context.Context, em.Message) error { return em.MarkPermanent(errors.New("bad record")) }, em.DeadLetter(failingDLQ, "jobs.dead.v1"), em.Recover())
	require.NoError(t, first.Register(topic, h))
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFirst()
	assert.ErrorIs(t, first.Run(firstCtx), em.ErrPublish)
	second, err := adapter.New(context.Background(), cfg(brokers, group))
	require.NoError(t, err)
	got := make(chan em.Message, 1)
	require.NoError(t, second.Register(topic, func(_ context.Context, m em.Message) error { got <- m; return nil }))
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSecond()
	done := make(chan error, 1)
	go func() { done <- second.Run(secondCtx) }()
	select {
	case m := <-got:
		assert.Equal(t, int64(0), m.Offset)
	case <-secondCtx.Done():
		t.Fatal("failed DLQ handoff lost the source record")
	}
	cancelSecond()
	require.NoError(t, <-done)
}
