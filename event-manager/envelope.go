package eventmanager

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Envelope is the JSON event contract shared with the Node kit.
type Envelope[T any] struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	EventVersion  int     `json:"eventVersion"`
	TenantCode    string  `json:"tenantCode"`
	OccurredAt    string  `json:"occurredAt"`
	Producer      string  `json:"producer"`
	CorrelationID *string `json:"correlationId,omitempty"`
	CausationID   *string `json:"causationId,omitempty"`
	TraceID       *string `json:"traceId,omitempty"`
	Payload       T       `json:"payload"`
}

// Event converts a typed envelope into a transport-independent raw envelope.
type Event interface {
	ToRaw() (Envelope[json.RawMessage], error)
}

// ToRaw marshals the payload while preserving envelope metadata.
func (e Envelope[T]) ToRaw() (Envelope[json.RawMessage], error) {
	p, err := json.Marshal(e.Payload)
	if err != nil {
		return Envelope[json.RawMessage]{}, fmt.Errorf("payload: %w", err)
	}
	return Envelope[json.RawMessage]{EventID: e.EventID, EventType: e.EventType, EventVersion: e.EventVersion, TenantCode: e.TenantCode, OccurredAt: e.OccurredAt, Producer: e.Producer, CorrelationID: e.CorrelationID, CausationID: e.CausationID, TraceID: e.TraceID, Payload: p}, nil
}

type correlationKey struct{}

// WithCorrelationID attaches a correlation ID to a context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the context's correlation ID, if present.
func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(correlationKey{}).(string)
	return v
}

// PrepareEvent fills generated fields on a copy and validates the Node wire contract.
func PrepareEvent(ctx context.Context, event Event, producer string) (Envelope[json.RawMessage], error) {
	if event == nil || (reflect.ValueOf(event).Kind() == reflect.Pointer && reflect.ValueOf(event).IsNil()) {
		return Envelope[json.RawMessage]{}, fmt.Errorf("%w: nil event", ErrInvalidEvent)
	}
	e, err := event.ToRaw()
	if err != nil {
		return e, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if e.EventID == "" {
		e.EventID, err = uuidV4()
		if err != nil {
			return e, err
		}
	}
	if e.OccurredAt == "" {
		e.OccurredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if e.Producer == "" {
		e.Producer = producer
	}
	if e.CorrelationID == nil || *e.CorrelationID == "" {
		id := CorrelationID(ctx)
		if id == "" {
			id, err = uuidV4()
			if err != nil {
				return e, err
			}
		}
		e.CorrelationID = &id
	}
	if err := ValidateEnvelope(e); err != nil {
		return e, err
	}
	return e, nil
}

// ValidateEnvelope checks required fields and timestamps on the shared JSON envelope.
func ValidateEnvelope(e Envelope[json.RawMessage]) error {
	if e.EventID == "" || e.EventType == "" || e.EventVersion <= 0 || e.Producer == "" {
		return fmt.Errorf("%w: eventId, eventType, positive eventVersion, and producer required", ErrInvalidEvent)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err != nil {
		return fmt.Errorf("%w: occurredAt: %v", ErrInvalidEvent, err)
	}
	if len(e.Payload) == 0 || string(e.Payload) == "null" || !json.Valid(e.Payload) {
		return fmt.Errorf("%w: payload required", ErrInvalidEvent)
	}
	return nil
}

func uuidV4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("uuid: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
