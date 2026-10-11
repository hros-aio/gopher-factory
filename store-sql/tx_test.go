package storesql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithinTxOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		callback error
		commit   bool
	}{
		{"commit", nil, true},
		{"rollback", errors.New("callback failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, mock := newMockClient(t)
			mock.ExpectBegin()
			if tc.commit {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err := c.WithinTx(context.Background(), func(ctx context.Context) error {
				db, dbErr := c.DB(ctx)
				require.NoError(t, dbErr)
				assert.NotNil(t, db)
				return tc.callback
			})
			if tc.callback == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.callback)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWithinTxPanicRollsBack(t *testing.T) {
	c, mock := newMockClient(t)
	mock.ExpectBegin()
	mock.ExpectRollback()
	assert.PanicsWithValue(t, "boom", func() {
		_ = c.WithinTx(context.Background(), func(context.Context) error { panic("boom") })
	})
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxNestedHandledErrorCommits(t *testing.T) {
	c, mock := newMockClient(t)
	mock.ExpectBegin()
	mock.ExpectCommit()
	cause := errors.New("non-SQL callback error")
	err := c.WithinTx(context.Background(), func(ctx context.Context) error {
		innerErr := c.WithinTx(ctx, func(inner context.Context) error {
			outerDB, dbErr := c.DB(ctx)
			require.NoError(t, dbErr)
			innerDB, dbErr := c.DB(inner)
			require.NoError(t, dbErr)
			assert.Same(t, outerDB.ConnPool, innerDB.ConnPool)
			return cause
		})
		assert.ErrorIs(t, innerErr, cause)
		return nil
	})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxCommitFailure(t *testing.T) {
	c, mock := newMockClient(t)
	cause := errors.New("transaction aborted")
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(cause)
	err := c.WithinTx(context.Background(), func(context.Context) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxForeignClient(t *testing.T) {
	first, firstMock := newMockClient(t)
	second, _ := newMockClient(t)
	firstMock.ExpectBegin()
	firstMock.ExpectRollback()
	err := first.WithinTx(context.Background(), func(ctx context.Context) error {
		_, dbErr := second.DB(ctx)
		assert.ErrorIs(t, dbErr, ErrForeignTransaction)
		return second.WithinTx(ctx, func(context.Context) error { return nil })
	})
	assert.ErrorIs(t, err, ErrForeignTransaction)
	assert.NoError(t, firstMock.ExpectationsWereMet())
}

func TestWithinTxRollbackFailurePreservesBothErrors(t *testing.T) {
	c, mock := newMockClient(t)
	callbackErr := errors.New("callback")
	rollbackErr := errors.New("rollback")
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(rollbackErr)
	err := c.WithinTx(context.Background(), func(context.Context) error { return callbackErr })
	require.Error(t, err)
	assert.ErrorIs(t, err, callbackErr)
	assert.ErrorIs(t, err, rollbackErr)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxPanicPreservedWhenRollbackFails(t *testing.T) {
	c, mock := newMockClient(t)
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))
	assert.PanicsWithValue(t, "original panic", func() {
		_ = c.WithinTx(context.Background(), func(context.Context) error { panic("original panic") })
	})
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestWithinTxRejectsInvalidInputs(t *testing.T) {
	c, _ := newMockClient(t)
	assert.Error(t, c.WithinTx(context.Background(), nil))
	var nilCtx context.Context
	assert.Error(t, c.WithinTx(nilCtx, func(context.Context) error { return nil }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, c.WithinTx(ctx, func(context.Context) error { return nil }), context.Canceled)
}
