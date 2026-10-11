package storesql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestErrorIdentities(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"closed", ErrClosed},
		{"invalid config", ErrInvalidConfig},
		{"foreign transaction", ErrForeignTransaction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.err)
			assert.ErrorIs(t, fmt.Errorf("operation: %w", tc.err), tc.err)
		})
	}
}

func TestSafeErrorPreservesCauseWithoutMessage(t *testing.T) {
	cause := errors.New("password=secret")
	err := safeError("connect PostgreSQL", cause)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.NotContains(t, err.Error(), "secret")
}

func TestErrorClass(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{gorm.ErrDuplicatedKey, "duplicate_key"},
		{context.Canceled, "canceled"},
		{context.DeadlineExceeded, "deadline"},
		{ErrClosed, "closed"},
		{errors.New("password=secret"), "other"},
	} {
		assert.Equal(t, tc.want, errorClass(tc.err))
	}
}
