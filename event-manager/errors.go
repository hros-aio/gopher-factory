package eventmanager

import "errors"

var (
	ErrClosed          = errors.New("event-manager: closed")
	ErrInvalidConfig   = errors.New("event-manager: invalid config")
	ErrInvalidEvent    = errors.New("event-manager: invalid event")
	ErrPublish         = errors.New("event-manager: publish failed")
	ErrProcess         = errors.New("event-manager: processing failed")
	ErrCommit          = errors.New("event-manager: commit failed")
	ErrShutdownTimeout = errors.New("event-manager: shutdown timeout")
	ErrState           = errors.New("event-manager: invalid state")
)

type classifiedError struct {
	error
	retryable bool
}

func (e classifiedError) Unwrap() error { return e.error }

// MarkRetryable identifies a handler failure that may be retried.
func MarkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{err, true}
}

// MarkPermanent identifies a handler failure that must not be retried.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return classifiedError{err, false}
}

// IsRetryable reports the classification on the outermost classification wrapper.
func IsRetryable(err error) bool { var c classifiedError; return errors.As(err, &c) && c.retryable }
