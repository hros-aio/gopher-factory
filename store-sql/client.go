package storesql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type clientState uint8

const (
	stateOpen clientState = iota
	stateClosing
	stateClosed
)

const startupPingTimeout = 5 * time.Second

// Client owns one PostgreSQL connection pool and its lifecycle.
type Client struct {
	db        *gorm.DB
	sqlDB     *sql.DB
	mu        sync.Mutex
	state     clientState
	active    int
	drained   chan struct{}
	closingCh chan struct{}
	closeDone chan struct{}
	closeErr  error
}

// New validates cfg, connects to PostgreSQL, and verifies startup readiness.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	return newWithDialector(ctx, cfg, nil, opts...)
}

func newWithDialector(ctx context.Context, cfg Config, dialector gorm.Dialector, opts ...Option) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("store-sql New: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store-sql New: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("store-sql New: %w", err)
	}
	settings := defaultOptions()
	if err := applyOptions(&settings, opts); err != nil {
		return nil, fmt.Errorf("store-sql New: %w", err)
	}
	if dialector == nil {
		dialector = postgres.Open(cfg.DSN)
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		DisableAutomaticPing: true,
		TranslateError:       true,
		Logger:               newGormLogger(settings.logger, cfg.SlowQueryThreshold),
	})
	if err != nil {
		return nil, safeError("open PostgreSQL", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, safeError("obtain PostgreSQL pool", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	c := &Client{db: db, sqlDB: sqlDB, state: stateOpen, closingCh: make(chan struct{})}
	if err := c.registerGate(); err != nil {
		_ = sqlDB.Close()
		return nil, safeError("install PostgreSQL lifecycle gate", err)
	}
	for _, plugin := range settings.plugins {
		if err := db.Use(plugin); err != nil {
			_ = sqlDB.Close()
			return nil, safeError("register PostgreSQL plugin", err)
		}
	}
	pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := sqlDB.PingContext(pingCtx); err != nil {
		_ = sqlDB.Close()
		return nil, safeError("ping PostgreSQL at startup", err)
	}
	return c, nil
}

// Ping checks current database readiness with ctx.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.beginOperation(ctx, false); err != nil {
		return err
	}
	defer c.endOperation()
	if err := c.sqlDB.PingContext(ctx); err != nil {
		return safeError("ping PostgreSQL", err)
	}
	return nil
}

// DB returns a context-bound GORM handle, joining the current Client transaction if present.
func (c *Client) DB(ctx context.Context) (*gorm.DB, error) {
	if c == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, errors.New("store-sql DB: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store-sql DB: %w", err)
	}
	tx := transactionFromContext(ctx)
	if tx != nil && tx.owner != c {
		return nil, ErrForeignTransaction
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stateOpen && !(c.state == stateClosing && tx != nil && tx.active.Load()) {
		return nil, ErrClosed
	}
	if tx != nil {
		if !tx.active.Load() {
			return nil, ErrClosed
		}
		return tx.db.WithContext(ctx), nil
	}
	return c.db.WithContext(ctx), nil
}

// Close rejects new work and waits for active work until ctx expires.
// If ctx expires, physical pool closure continues and a later Close can await it.
func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("store-sql Close: context is nil")
	}
	c.mu.Lock()
	if c.state == stateOpen {
		c.state = stateClosing
		c.drained = make(chan struct{})
		c.closeDone = make(chan struct{})
		close(c.closingCh)
		if c.active == 0 {
			close(c.drained)
		}
		go c.finishClose(ctx)
	}
	done := c.closeDone
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("close PostgreSQL: %w", err)
	}
	select {
	case <-done:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	default:
	}
	select {
	case <-done:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return fmt.Errorf("close PostgreSQL: %w", ctx.Err())
	}
}

func (c *Client) finishClose(ctx context.Context) {
	select {
	case <-c.drained:
	case <-ctx.Done():
	}
	err := c.sqlDB.Close()
	c.mu.Lock()
	if err != nil {
		c.closeErr = safeError("close PostgreSQL pool", err)
	}
	c.state = stateClosed
	close(c.closeDone)
	c.mu.Unlock()
}

func (c *Client) beginOperation(ctx context.Context, allowTransaction bool) error {
	if c == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("store-sql operation: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store-sql operation: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == stateOpen {
		c.active++
		return nil
	}
	if allowTransaction && c.state == stateClosing {
		if tx := transactionFromContext(ctx); tx != nil && tx.owner == c && tx.active.Load() {
			c.active++
			return nil
		}
	}
	return ErrClosed
}

func (c *Client) endOperation() {
	c.mu.Lock()
	c.active--
	if c.state == stateClosing && c.active == 0 {
		close(c.drained)
	}
	c.mu.Unlock()
}

const trackedStatementKey = "storesql:tracked-statement"

func (c *Client) registerGate() error {
	before := func(db *gorm.DB) {
		if err := c.beginOperation(db.Statement.Context, true); err != nil {
			db.AddError(err)
			return
		}
		db.Statement.Settings.Store(trackedStatementKey, true)
	}
	after := func(db *gorm.DB) {
		if _, ok := db.Statement.Settings.LoadAndDelete(trackedStatementKey); ok {
			c.endOperation()
		}
	}
	registrations := []func() error{
		func() error {
			return c.db.Callback().Create().Before("*").Register("storesql:before_create", before)
		},
		func() error {
			return c.db.Callback().Create().After("*").Register("storesql:after_create", after)
		},
		func() error {
			return c.db.Callback().Query().Before("*").Register("storesql:before_query", before)
		},
		func() error {
			return c.db.Callback().Query().After("*").Register("storesql:after_query", after)
		},
		func() error {
			return c.db.Callback().Update().Before("*").Register("storesql:before_update", before)
		},
		func() error {
			return c.db.Callback().Update().After("*").Register("storesql:after_update", after)
		},
		func() error {
			return c.db.Callback().Delete().Before("*").Register("storesql:before_delete", before)
		},
		func() error {
			return c.db.Callback().Delete().After("*").Register("storesql:after_delete", after)
		},
		func() error { return c.db.Callback().Raw().Before("*").Register("storesql:before_raw", before) },
		func() error { return c.db.Callback().Raw().After("*").Register("storesql:after_raw", after) },
		func() error { return c.db.Callback().Row().Before("*").Register("storesql:before_row", before) },
		func() error { return c.db.Callback().Row().After("*").Register("storesql:after_row", after) },
	}
	for _, register := range registrations {
		if err := register(); err != nil {
			return fmt.Errorf("register GORM callback: %w", err)
		}
	}
	return nil
}
