package eventmanager

import "encoding/json"

// Message is a broker-independent record. Call Clone before retaining it.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Envelope  *Envelope[json.RawMessage]
}

// Clone copies the mutable raw bytes and decoded payload.
func (m Message) Clone() Message {
	m.Key = append([]byte(nil), m.Key...)
	m.Value = append([]byte(nil), m.Value...)
	if m.Envelope != nil {
		e := *m.Envelope
		e.Payload = append(json.RawMessage(nil), e.Payload...)
		copyString := func(p *string) *string {
			if p == nil {
				return nil
			}
			v := *p
			return &v
		}
		e.CorrelationID = copyString(e.CorrelationID)
		e.CausationID = copyString(e.CausationID)
		e.TraceID = copyString(e.TraceID)
		m.Envelope = &e
	}
	return m
}
