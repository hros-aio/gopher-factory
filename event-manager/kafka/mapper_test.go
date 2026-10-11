package kafka

import (
	"encoding/json"
	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestDecodeNodeFixture(t *testing.T) {
	wire := []byte(`{"eventId":"id","eventType":"created","eventVersion":1,"tenantCode":"","occurredAt":"2026-01-01T00:00:00Z","producer":"node","payload":{"x":1},"correlationId":null}`)
	m, err := decodeRecord(&kgo.Record{Topic: "sales.created.v1", Partition: 2, Offset: 3, Key: []byte("k"), Value: wire})
	require.NoError(t, err)
	assert.Equal(t, "node", m.Envelope.Producer)
	assert.Nil(t, m.Envelope.CorrelationID)
	b, err := encodeEvent(*m.Envelope)
	require.NoError(t, err)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Contains(t, got, "tenantCode")
	assert.NotContains(t, got, "correlationId")
	assert.Equal(t, int32(2), m.Partition)
}
func TestDecodeRejectsMissingTenant(t *testing.T) {
	m, err := decodeRecord(&kgo.Record{Value: []byte(`{"eventId":"id","eventType":"created","eventVersion":1,"occurredAt":"2026-01-01T00:00:00Z","producer":"node","payload":1}`)})
	assert.ErrorIs(t, err, em.ErrInvalidEvent)
	assert.Nil(t, m.Envelope)
}
