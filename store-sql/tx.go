package storesql

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"gorm.io/gorm"
)

type transactionKey struct{}

type transactionState struct {
	owner  *Client
	db     *gorm.DB
	active atomic.Bool
}

func transactionFromContext(ctx context.Context) *transactionState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(transactionKey{}).(*transactionState)
	return state
}

// WithinTx runs fn in one transaction, joining an existing transaction in ctx.
func (c *Client) WithinTx(ctx context.Context, fn func(context.Context) error) (err error) {
	if c == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("store-sql WithinTx: context is nil")
	}
	if fn == nil {
		return errors.New("store-sql WithinTx: callback is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store-sql WithinTx: %w", err)
	}
	if existing := transactionFromContext(ctx); existing != nil {
		if existing.owner != c {
			return ErrForeignTransaction
		}
		if !existing.active.Load() {
			return ErrClosed
		}
		return fn(ctx)
	}
	if err := c.beginOperation(ctx, false); err != nil {
		return err
	}
	defer c.endOperation()
	tx := c.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return fmt.Errorf("begin PostgreSQL transaction: %w", tx.Error)
	}
	state := &transactionState{owner: c, db: tx}
	state.active.Store(true)
	txCtx := context.WithValue(ctx, transactionKey{}, state)
	defer state.active.Store(false)
	defer func() {
		if panicValue := recover(); panicValue != nil {
			_ = tx.Rollback().Error
			panic(panicValue)
		}
	}()
	err = fn(txCtx)
	if err != nil {
		if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
			return errors.Join(fmt.Errorf("transaction callback: %w", err), fmt.Errorf("rollback PostgreSQL transaction: %w", rollbackErr))
		}
		return fmt.Errorf("transaction callback: %w", err)
	}
	if commitErr := tx.Commit().Error; commitErr != nil {
		return fmt.Errorf("commit PostgreSQL transaction: %w", commitErr)
	}
	return nil
}
