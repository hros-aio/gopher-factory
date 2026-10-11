package eventmanager

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareEventNodeWire(t *testing.T) {
	e := Envelope[map[string]int]{EventType: "invoice.paid", EventVersion: 1, Payload: map[string]int{"amount": 7}}
	ctx := WithCorrelationID(context.Background(), "corr-1")
	raw, err := PrepareEvent(ctx, e, "worker")
	require.NoError(t, err)
	assert.True(t, regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(raw.EventID))
	assert.Equal(t, "corr-1", *raw.CorrelationID)
	assert.Equal(t, "worker", raw.Producer)
	assert.Empty(t, e.EventID)
	b, err := json.Marshal(raw)
	require.NoError(t, err)
	assert.JSONEq(t, `{"eventId":"`+raw.EventID+`","eventType":"invoice.paid","eventVersion":1,"tenantCode":"","occurredAt":"`+raw.OccurredAt+`","producer":"worker","correlationId":"corr-1","payload":{"amount":7}}`, string(b))
}
func TestValidateEnvelope(t *testing.T) {
	e := Envelope[json.RawMessage]{EventID: "id", EventType: "t", EventVersion: 1, OccurredAt: "2026-01-01T00:00:00Z", Producer: "p", Payload: json.RawMessage(`{}`)}
	require.NoError(t, ValidateEnvelope(e))
	e.Payload = nil
	assert.ErrorIs(t, ValidateEnvelope(e), ErrInvalidEvent)
}
func TestPrepareEventInvalid(t *testing.T) {
	_, err := PrepareEvent(context.Background(), nil, "worker")
	assert.ErrorIs(t, err, ErrInvalidEvent)
	var nilEnvelope *Envelope[int]
	_, err = PrepareEvent(context.Background(), nilEnvelope, "worker")
	assert.ErrorIs(t, err, ErrInvalidEvent)
	_, err = PrepareEvent(context.Background(), Envelope[func()]{EventType: "bad", EventVersion: 1, Payload: func() {}}, "worker")
	assert.ErrorIs(t, err, ErrInvalidEvent)
	_, err = PrepareEvent(context.Background(), Envelope[int]{EventType: "bad", EventVersion: 0, Payload: 1}, "worker")
	assert.ErrorIs(t, err, ErrInvalidEvent)
	_, err = PrepareEvent(context.Background(), Envelope[int]{EventType: "ok", EventVersion: 1, OccurredAt: "yesterday", Payload: 1}, "worker")
	assert.ErrorIs(t, err, ErrInvalidEvent)
}
