package eventtest

import (
	"context"
	"errors"
	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFakeSubscriber(t *testing.T) {
	s := NewFakeSubscriber()
	want := errors.New("fail")
	require.NoError(t, s.Register("billing.paid.v1", func(context.Context, em.Message) error { return want }))
	assert.ErrorIs(t, s.Register("billing.paid.v1", func(context.Context, em.Message) error { return nil }), em.ErrState)
	assert.ErrorIs(t, s.Deliver(context.Background(), em.Message{Topic: "billing.paid.v1"}), want)
	assert.False(t, s.Deliveries()[0].Committed)
	require.NoError(t, s.Close(context.Background()))
	assert.ErrorIs(t, s.Ping(context.Background()), em.ErrClosed)
}
func TestFakeSubscriberSuccessAndCancellation(t *testing.T) {
	s := NewFakeSubscriber()
	require.NoError(t, s.Register("billing.paid.v1", func(context.Context, em.Message) error { return nil }))
	require.NoError(t, s.Deliver(context.Background(), em.Message{Topic: "billing.paid.v1"}))
	assert.True(t, s.Deliveries()[0].Committed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, s.Run(ctx))
	assert.ErrorIs(t, s.Run(ctx), em.ErrState)
	assert.ErrorIs(t, s.Register("billing.other.v1", func(context.Context, em.Message) error { return nil }), em.ErrState)
	assert.ErrorIs(t, s.Deliver(ctx, em.Message{Topic: "billing.paid.v1"}), context.Canceled)
}
func TestFakeSubscriberInvalid(t *testing.T) {
	s := NewFakeSubscriber()
	assert.ErrorIs(t, s.Register("bad", func(context.Context, em.Message) error { return nil }), em.ErrInvalidConfig)
	assert.ErrorIs(t, s.Register("billing.paid.v1", nil), em.ErrInvalidConfig)
	assert.ErrorIs(t, s.Deliver(context.Background(), em.Message{Topic: "billing.missing.v1"}), em.ErrInvalidConfig)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, s.Ping(ctx), context.Canceled)
	assert.ErrorIs(t, s.Close(ctx), context.Canceled)
}

func TestWorkerWiringWithoutKafka(t *testing.T) {
	ctx := context.Background()
	var publisher em.Publisher = NewFakePublisher()
	var subscriber em.Subscriber = NewFakeSubscriber()
	handler := em.Chain(func(context.Context, em.Message) error { return nil }, em.Recover())
	require.NoError(t, subscriber.Register("billing.paid.v1", handler))
	require.NoError(t, publisher.Publish(ctx, "billing.result.v1", "tenant:agg", em.Envelope[int]{EventType: "result", EventVersion: 1, Payload: 1}))
	require.NoError(t, subscriber.(*FakeSubscriber).Deliver(ctx, em.Message{Topic: "billing.paid.v1"}))
	require.NoError(t, publisher.Ping(ctx))
	require.NoError(t, subscriber.Close(ctx))
	require.NoError(t, publisher.Close(ctx))
}
