package storesql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newMockClient(t *testing.T) (*Client, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	mock.ExpectPing()
	c, err := newWithDialector(context.Background(), Config{DSN: testDSN}, postgres.New(postgres.Config{Conn: sqlDB}))
	require.NoError(t, err)
	t.Cleanup(func() {
		c.mu.Lock()
		open := c.state == stateOpen
		c.mu.Unlock()
		if open {
			mock.ExpectClose()
		}
		assert.NoError(t, c.Close(context.Background()))
	})
	return c, mock
}

func TestNewValidatesBeforeIO(t *testing.T) {
	_, err := New(context.Background(), Config{DSN: "postgres://%"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewRejectsInvalidOptionBeforeIO(t *testing.T) {
	_, err := New(context.Background(), Config{DSN: testDSN}, WithLogger(nil))
	require.Error(t, err)
	assert.ErrorContains(t, err, "logger is nil")
}

func TestNewPingFailureIsWrappedAndRedacted(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer func() { _ = sqlDB.Close() }()
	cause := errors.New("password=secret")
	mock.ExpectPing().WillReturnError(cause)
	c, err := newWithDialector(context.Background(), Config{DSN: testDSN}, postgres.New(postgres.Config{Conn: sqlDB}))
	assert.Nil(t, c)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.NotContains(t, err.Error(), "secret")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClientPingAndContextBoundDB(t *testing.T) {
	c, mock := newMockClient(t)

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "value")
	db, err := c.DB(ctx)
	require.NoError(t, err)
	assert.Same(t, ctx, db.Statement.Context)
	mock.ExpectPing()
	assert.NoError(t, c.Ping(ctx))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestNewDoesNotMutateCallerConfig(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	mock.ExpectPing()
	cfg := Config{DSN: testDSN}
	c, err := newWithDialector(context.Background(), cfg, postgres.New(postgres.Config{Conn: sqlDB}))
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()
	assert.Zero(t, cfg.MaxOpenConns)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClientPingReportsLostConnection(t *testing.T) {
	c, mock := newMockClient(t)
	cause := errors.New("connection lost")
	mock.ExpectPing().WillReturnError(cause)
	err := c.Ping(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClientCloseWaitsForActiveWork(t *testing.T) {
	defer goleak.VerifyNone(t)
	c, mock := newMockClient(t)
	ctx := context.Background()
	require.NoError(t, c.beginOperation(ctx, false))
	mock.ExpectClose()
	result := make(chan error, 1)
	go func() { result <- c.Close(ctx) }()
	<-c.closingCh
	select {
	case err := <-result:
		t.Fatalf("Close returned before active work ended: %v", err)
	default:
	}
	c.endOperation()
	assert.NoError(t, <-result)
	_, err := c.DB(ctx)
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, c.Ping(ctx), ErrClosed)
	assert.NoError(t, c.Close(ctx))
}

func TestBorrowedHandleRejectsOperationsAfterClose(t *testing.T) {
	c, mock := newMockClient(t)
	db, err := c.DB(context.Background())
	require.NoError(t, err)
	mock.ExpectClose()
	require.NoError(t, c.Close(context.Background()))
	assert.ErrorIs(t, db.Exec("SELECT 1").Error, ErrClosed)
}

func TestClientCloseDeadlineContinuesClosure(t *testing.T) {
	defer goleak.VerifyNone(t)
	c, mock := newMockClient(t)
	require.NoError(t, c.beginOperation(context.Background(), false))
	mock.ExpectClose()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Close(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	c.endOperation()
	assert.NoError(t, c.Close(context.Background()))
}

func TestClientRejectsCanceledContext(t *testing.T) {
	c, _ := newMockClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	_, err := c.DB(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, c.Ping(ctx), context.DeadlineExceeded)
}

func TestClientPluginFailureClosesPool(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	cause := errors.New("plugin failed with secret")
	c, err := newWithDialector(context.Background(), Config{DSN: testDSN},
		postgres.New(postgres.Config{Conn: sqlDB}), WithPlugin(testPlugin{err: cause}))
	assert.Nil(t, c)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.NotContains(t, err.Error(), "secret")
	assert.NoError(t, mock.ExpectationsWereMet())
	assert.Error(t, sqlDB.Ping())
}

func TestClientDuplicatePluginFails(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	c, err := newWithDialector(context.Background(), Config{DSN: testDSN},
		postgres.New(postgres.Config{Conn: sqlDB}), WithPlugin(testPlugin{}), WithPlugin(testPlugin{}))
	assert.Nil(t, c)
	require.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClientDuplicateKeyTranslation(t *testing.T) {
	c, mock := newMockClient(t)
	db, err := c.DB(context.Background())
	require.NoError(t, err)
	mock.ExpectExec("INSERT INTO items").WillReturnError(&pgconn.PgError{Code: "23505"})
	err = db.Exec("INSERT INTO items(id) VALUES (?)", 1).Error
	assert.ErrorIs(t, err, gorm.ErrDuplicatedKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}
