package eventtest

import (
	"context"
	"errors"
	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFakePublisher(t *testing.T) {
	p := NewFakePublisher()
	require.NoError(t, p.Publish(context.Background(), "billing.paid.v1", "k", em.Envelope[int]{EventType: "paid", EventVersion: 1, Payload: 1}))
	records := p.Records()
	require.Len(t, records, 1)
	records[0].Envelope.Payload[0] = 'x'
	assert.JSONEq(t, "1", string(p.Records()[0].Envelope.Payload))
	require.NoError(t, p.Close(context.Background()))
	assert.ErrorIs(t, p.Ping(context.Background()), em.ErrClosed)
}
func TestFakePublisherErrors(t *testing.T) {
	p := NewFakePublisher()
	cause := errors.New("broker")
	p.PublishError = cause
	assert.ErrorIs(t, p.Publish(context.Background(), "billing.paid.v1", "k", em.Envelope[int]{Payload: 1}), cause)
	assert.Empty(t, p.Records())
	p.PublishError = nil
	p.PingError = cause
	assert.ErrorIs(t, p.Ping(context.Background()), cause)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, p.Close(ctx), context.Canceled)
	assert.ErrorIs(t, p.Publish(ctx, "billing.paid.v1", "k", nil), context.Canceled)
}
