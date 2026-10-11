package eventmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"testing"
	"time"
)

type recordingPublisher struct {
	calls int
	err   error
}

func (p *recordingPublisher) Publish(context.Context, string, string, Event) error {
	p.calls++
	return p.err
}
func (p *recordingPublisher) Ping(context.Context) error  { return nil }
func (p *recordingPublisher) Close(context.Context) error { return nil }
func TestRecoverRetryDLQ(t *testing.T) {
	p := &recordingPublisher{}
	calls := 0
	h := Chain(func(context.Context, Message) error {
		calls++
		if calls == 1 {
			panic("boom")
		}
		return MarkRetryable(errors.New("retry"))
	}, DeadLetter(p, "workers.failed.v1"), Retry(RetryConfig{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond, Rand: func(int64) int64 { return 0 }, Wait: func(context.Context, time.Duration) error { return nil }}), Recover())
	require.NoError(t, h(context.Background(), Message{Topic: "workers.jobs.v1"}))
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, p.calls)
}
func TestRetryOnlyMarked(t *testing.T) {
	calls := 0
	h := Retry(RetryConfig{})(func(context.Context, Message) error { calls++; return errors.New("plain") })
	assert.Error(t, h(context.Background(), Message{}))
	assert.Equal(t, 1, calls)
}
func TestRetryMarkedAndCancellation(t *testing.T) {
	calls := 0
	waits := 0
	cfg := RetryConfig{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, Rand: func(n int64) int64 { assert.Greater(t, n, int64(0)); return 0 }, Wait: func(context.Context, time.Duration) error { waits++; return nil }}
	h := Retry(cfg)(func(context.Context, Message) error {
		calls++
		if calls < 3 {
			return MarkRetryable(errors.New("transient"))
		}
		return nil
	})
	require.NoError(t, h(context.Background(), Message{}))
	assert.Equal(t, 3, calls)
	assert.Equal(t, 2, waits)
	cfg.Wait = func(context.Context, time.Duration) error { return context.Canceled }
	h = Retry(cfg)(func(context.Context, Message) error { return MarkRetryable(errors.New("transient")) })
	assert.ErrorIs(t, h(context.Background(), Message{}), context.Canceled)
	assert.ErrorIs(t, (RetryConfig{MaxAttempts: -1}).Validate(), ErrInvalidConfig)
}

type memoryStore struct {
	done               bool
	lookupErr, markErr error
	marks              int
}

func (s *memoryStore) Completed(context.Context, string) (bool, error) { return s.done, s.lookupErr }
func (s *memoryStore) MarkCompleted(context.Context, string) error     { s.marks++; return s.markErr }
func TestIdempotency(t *testing.T) {
	s := &memoryStore{}
	calls := 0
	m := Message{Envelope: &Envelope[json.RawMessage]{EventID: "id"}}
	h := Idempotency(s)(func(context.Context, Message) error { calls++; return nil })
	require.NoError(t, h(context.Background(), m))
	assert.Equal(t, 1, s.marks)
	s.done = true
	require.NoError(t, h(context.Background(), m))
	assert.Equal(t, 1, calls)
	s.done = false
	s.lookupErr = errors.New("store unavailable")
	assert.ErrorIs(t, h(context.Background(), m), ErrProcess)
	s.lookupErr = nil
	s.markErr = errors.New("mark unavailable")
	assert.ErrorIs(t, h(context.Background(), m), s.markErr)
}
func TestDeadLetterFailedPublish(t *testing.T) {
	p := &recordingPublisher{err: errors.New("broker down")}
	h := DeadLetter(p, "jobs.dead.v1")(func(context.Context, Message) error { return MarkPermanent(errors.New("secret=abc")) })
	assert.ErrorIs(t, h(context.Background(), Message{Topic: "jobs.work.v1"}), ErrPublish)
	assert.Equal(t, 1, p.calls)
}
func TestLoggingMetadata(t *testing.T) {
	var b bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&b, nil))
	id := "corr"
	m := Message{Topic: "jobs.work.v1", Partition: 2, Offset: 4, Value: []byte("sensitive"), Envelope: &Envelope[json.RawMessage]{CorrelationID: &id}}
	require.NoError(t, Logging(logger)(func(context.Context, Message) error { return nil })(context.Background(), m))
	assert.Contains(t, b.String(), "corr")
	assert.NotContains(t, b.String(), "sensitive")
}
func TestWaitContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, waitContext(ctx, time.Second), context.Canceled)
	require.NoError(t, waitContext(context.Background(), 0))
}
func TestPublishDeadLetterMetadata(t *testing.T) {
	p := &capturingPublisher{}
	e := Envelope[json.RawMessage]{EventID: "source-id", EventType: "created", EventVersion: 1, TenantCode: "tenant", OccurredAt: "2026-01-01T00:00:00Z", Producer: "node", Payload: json.RawMessage(`{"x":1}`)}
	m := Message{Topic: "jobs.work.v1", Partition: 2, Offset: 4, Key: []byte("key"), Value: []byte("raw"), Envelope: &e}
	require.NoError(t, PublishDeadLetter(context.Background(), p, "jobs.dead.v1", m, MarkPermanent(errors.New("password=secret"))))
	assert.Equal(t, "jobs.dead.v1", p.topic)
	var data DeadLetterData
	require.NoError(t, json.Unmarshal(p.event.Payload, &data))
	assert.Equal(t, "permanent", data.FailureClass)
	assert.Equal(t, "handler failed", data.FailureMessage)
	assert.Equal(t, "source-id", data.Envelope.EventID)
	assert.Equal(t, int32(2), data.Partition)
	assert.NotContains(t, string(p.event.Payload), "password=secret")
}
func TestPublishDeadLetterInvalidEnvelopeClass(t *testing.T) {
	p := &capturingPublisher{}
	require.NoError(t, PublishDeadLetter(context.Background(), p, "jobs.dead.v1", Message{Topic: "jobs.work.v1", Value: []byte("bad")}, ErrInvalidEvent))
	var data DeadLetterData
	require.NoError(t, json.Unmarshal(p.event.Payload, &data))
	assert.Equal(t, "invalid_event", data.FailureClass)
	assert.Equal(t, "invalid event envelope", data.FailureMessage)
	assert.Nil(t, data.Envelope)
}

type capturingPublisher struct {
	topic string
	event Envelope[json.RawMessage]
}

func (p *capturingPublisher) Publish(_ context.Context, topic, _ string, event Event) error {
	p.topic = topic
	var err error
	p.event, err = event.ToRaw()
	return err
}
func (p *capturingPublisher) Ping(context.Context) error  { return nil }
func (p *capturingPublisher) Close(context.Context) error { return nil }
