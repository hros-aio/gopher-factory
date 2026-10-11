package eventmanager

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageClone(t *testing.T) {
	m := Message{Key: []byte("a"), Value: []byte("b"), Envelope: &Envelope[json.RawMessage]{Payload: json.RawMessage(`{"x":1}`)}}
	c := m.Clone()
	c.Key[0] = 'z'
	c.Value[0] = 'z'
	c.Envelope.Payload[0] = 'x'
	assert.Equal(t, "a", string(m.Key))
	assert.Equal(t, "b", string(m.Value))
	assert.JSONEq(t, `{"x":1}`, string(m.Envelope.Payload))
}

func TestMessageCloneOptionalMetadata(t *testing.T) {
	id := "original"
	m := Message{Envelope: &Envelope[json.RawMessage]{CorrelationID: &id}}
	copy := m.Clone()
	*copy.Envelope.CorrelationID = "changed"
	assert.Equal(t, "original", *m.Envelope.CorrelationID)
}
