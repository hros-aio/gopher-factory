package eventmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"
)

// Chain applies middleware in order, with the first wrapper outermost.
func Chain(h Handler, middleware ...Middleware) Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}

// Recover converts a handler panic to a processing error.
func Recover() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m Message) (err error) {
			defer func() {
				if v := recover(); v != nil {
					err = fmt.Errorf("%w: panic: %v", ErrProcess, v)
				}
			}()
			return next(ctx, m)
		}
	}
}

// Logging emits record coordinates and correlation ID without logging payloads.
func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, m Message) error {
			start := time.Now()
			err := next(ctx, m)
			id := ""
			if m.Envelope != nil && m.Envelope.CorrelationID != nil {
				id = *m.Envelope.CorrelationID
			}
			status := "success"
			if err != nil {
				status = "failed"
			}
			logger.InfoContext(ctx, "event processed", "topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "correlation_id", id, "duration", time.Since(start), "status", status)
			return err
		}
	}
}

// RetryConfig controls total attempts and exponential full-jitter backoff.
type RetryConfig struct {
	MaxAttempts                int
	InitialBackoff, MaxBackoff time.Duration
	Rand                       func(int64) int64
	Wait                       func(context.Context, time.Duration) error
}

func (c RetryConfig) defaults() RetryConfig {
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.InitialBackoff == 0 {
		c.InitialBackoff = 200 * time.Millisecond
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 2 * time.Second
	}
	if c.Rand == nil {
		c.Rand = rand.Int63n
	}
	if c.Wait == nil {
		c.Wait = waitContext
	}
	return c
}

// Validate checks retry configuration after applying zero-value defaults.
func (c RetryConfig) Validate() error {
	if c.MaxAttempts < 0 || c.InitialBackoff < 0 || c.MaxBackoff < 0 {
		return fmt.Errorf("%w: negative retry setting", ErrInvalidConfig)
	}
	c = c.defaults()
	if c.MaxBackoff < c.InitialBackoff {
		return fmt.Errorf("%w: max backoff below initial", ErrInvalidConfig)
	}
	return nil
}
func waitContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type attemptsKey struct{}

// Attempts returns the number of handler attempts in the current retry chain.
func Attempts(ctx context.Context) int {
	n, _ := ctx.Value(attemptsKey{}).(*int)
	if n == nil {
		return 1
	}
	return *n
}

// Retry repeats only explicitly retryable failures. The first call counts as an attempt.
func Retry(cfg RetryConfig) Middleware {
	cfg = cfg.defaults()
	return func(next Handler) Handler {
		return func(ctx context.Context, m Message) error {
			if err := cfg.Validate(); err != nil {
				return err
			}
			nptr, _ := ctx.Value(attemptsKey{}).(*int)
			if nptr == nil {
				nptr = new(int)
				ctx = context.WithValue(ctx, attemptsKey{}, nptr)
			}
			var err error
			for *nptr < cfg.MaxAttempts {
				*nptr++
				err = next(ctx, m)
				if err == nil || !IsRetryable(err) || *nptr == cfg.MaxAttempts {
					return err
				}
				d := cfg.InitialBackoff
				for i := 1; i < *nptr && d < cfg.MaxBackoff; i++ {
					if d > cfg.MaxBackoff/2 {
						d = cfg.MaxBackoff
						break
					}
					d *= 2
				}
				if d > cfg.MaxBackoff {
					d = cfg.MaxBackoff
				}
				if werr := cfg.Wait(ctx, time.Duration(cfg.Rand(int64(d)+1))); werr != nil {
					return fmt.Errorf("%w: retry canceled: %w", ErrProcess, werr)
				}
			}
			return err
		}
	}
}

// DeadLetterData preserves the source record and failure metadata for replay.
type DeadLetterData struct {
	Topic          string         `json:"topic"`
	Partition      int32          `json:"partition"`
	Offset         int64          `json:"offset"`
	Key            []byte         `json:"key"`
	RawValue       []byte         `json:"rawValue"`
	Envelope       *Envelope[any] `json:"envelope,omitempty"`
	FailureClass   string         `json:"failureClass"`
	FailureMessage string         `json:"failureMessage"`
	Attempts       int            `json:"attempts"`
	FailedAt       string         `json:"failedAt"`
}

// PublishDeadLetter sends a failed source record and returns only after acknowledgment.
func PublishDeadLetter(ctx context.Context, pub Publisher, topic string, m Message, cause error) error {
	if pub == nil || topic == "" {
		return fmt.Errorf("%w: DLQ unavailable: %w", ErrProcess, cause)
	}
	class := "unclassified"
	if errors.Is(cause, ErrInvalidEvent) {
		class = "invalid_event"
	} else if IsRetryable(cause) {
		class = "retryable"
	}
	var ce classifiedError
	if errors.As(cause, &ce) && !ce.retryable {
		class = "permanent"
	}
	message := "handler failed"
	if errors.Is(cause, ErrInvalidEvent) {
		message = "invalid event envelope"
	}
	data := DeadLetterData{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Key: append([]byte(nil), m.Key...), RawValue: append([]byte(nil), m.Value...), FailureClass: class, FailureMessage: message, Attempts: Attempts(ctx), FailedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if m.Envelope != nil {
		var payload any
		_ = json.Unmarshal(m.Envelope.Payload, &payload)
		e := Envelope[any]{EventID: m.Envelope.EventID, EventType: m.Envelope.EventType, EventVersion: m.Envelope.EventVersion, TenantCode: m.Envelope.TenantCode, OccurredAt: m.Envelope.OccurredAt, Producer: m.Envelope.Producer, CorrelationID: m.Envelope.CorrelationID, CausationID: m.Envelope.CausationID, TraceID: m.Envelope.TraceID, Payload: payload}
		data.Envelope = &e
	}
	e := Envelope[DeadLetterData]{EventType: "event.failed", EventVersion: 1, TenantCode: "", Payload: data}
	if err := pub.Publish(ctx, topic, string(m.Key), e); err != nil {
		return fmt.Errorf("%w: DLQ publish: %w", ErrPublish, err)
	}
	return nil
}

// DeadLetter acknowledges a failed handler only after its original record reaches the DLQ.
func DeadLetter(pub Publisher, topic string) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m Message) error {
			n := 0
			ctx = context.WithValue(ctx, attemptsKey{}, &n)
			err := next(ctx, m)
			if err == nil {
				return nil
			}
			return PublishDeadLetter(ctx, pub, topic, m, err)
		}
	}
}

// IdempotencyStore is caller-owned durable completion storage.
type IdempotencyStore interface {
	Completed(context.Context, string) (bool, error)
	MarkCompleted(context.Context, string) error
}

// Idempotency skips completed event IDs and marks only after successful handling or DLQ handoff.
func Idempotency(store IdempotencyStore) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, m Message) error {
			if store == nil || m.Envelope == nil || m.Envelope.EventID == "" {
				return next(ctx, m)
			}
			id := m.Envelope.EventID
			done, err := store.Completed(ctx, id)
			if err != nil {
				return fmt.Errorf("%w: idempotency lookup: %w", ErrProcess, err)
			}
			if done {
				return nil
			}
			if err = next(ctx, m); err != nil {
				return err
			}
			if err = store.MarkCompleted(ctx, id); err != nil {
				return fmt.Errorf("%w: idempotency mark: %w", ErrProcess, err)
			}
			return nil
		}
	}
}
