package kafka

import (
	"encoding/json"
	"fmt"

	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/twmb/franz-go/pkg/kgo"
)

func encodeEvent(e em.Envelope[json.RawMessage]) ([]byte, error) {
	if err := em.ValidateEnvelope(e); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %w", em.ErrInvalidEvent, err)
	}
	return b, nil
}
func decodeRecord(r *kgo.Record) (em.Message, error) {
	m := em.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), r.Value...)}
	var e em.Envelope[json.RawMessage]
	if err := json.Unmarshal(r.Value, &e); err != nil {
		return m, fmt.Errorf("%w: decode: %w", em.ErrInvalidEvent, err)
	}
	if err := em.ValidateEnvelope(e); err != nil {
		return m, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.Value, &fields); err != nil {
		return m, err
	}
	if _, ok := fields["tenantCode"]; !ok {
		return m, fmt.Errorf("%w: tenantCode required", em.ErrInvalidEvent)
	}
	m.Envelope = &e
	return m, nil
}
