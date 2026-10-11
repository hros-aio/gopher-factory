// Package storesql provides a PostgreSQL connection kit for Go workers.
package storesql

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

var (
	// ErrClosed indicates that the client is closing or closed.
	ErrClosed = errors.New("store-sql client closed")
	// ErrInvalidConfig indicates an invalid configuration field.
	ErrInvalidConfig = errors.New("invalid store-sql configuration")
	// ErrForeignTransaction indicates a transaction context owned by another client.
	ErrForeignTransaction = errors.New("transaction belongs to another store-sql client")
)

type redactedError struct {
	cause error
}

func (e redactedError) Error() string { return "operation failed" }
func (e redactedError) Unwrap() error { return e.cause }

func safeError(operation string, cause error) error {
	return fmt.Errorf("%s: %w", operation, redactedError{cause: cause})
}

func errorClass(err error) string {
	switch {
	case errors.Is(err, gorm.ErrDuplicatedKey):
		return "duplicate_key"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrClosed):
		return "closed"
	default:
		return "other"
	}
}
