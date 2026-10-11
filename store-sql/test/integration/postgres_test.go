//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	storesql "github.com/hros-aio/gopher-factory/store-sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"gorm.io/gorm"
)

type integrationPlugin struct{}

func (integrationPlugin) Name() string              { return "integration-plugin" }
func (integrationPlugin) Initialize(*gorm.DB) error { return nil }

func TestPostgreSQLLifecycleTransactionsAndDuplicateKey(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("worker"),
		postgres.WithUsername("worker"),
		postgres.WithPassword("worker"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, container.Terminate(context.Background())) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	var logs bytes.Buffer
	client, err := storesql.New(ctx, storesql.Config{DSN: dsn},
		storesql.WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		storesql.WithPlugin(integrationPlugin{}))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close(context.Background())) })
	assert.NoError(t, client.Ping(ctx))
	db, err := client.DB(ctx)
	require.NoError(t, err)
	assert.Contains(t, db.Config.Plugins, "integration-plugin")
	require.NoError(t, db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").Error)

	require.NoError(t, client.WithinTx(ctx, func(txCtx context.Context) error {
		txDB, txErr := client.DB(txCtx)
		if txErr != nil {
			return txErr
		}
		return txDB.Exec("INSERT INTO items(id, name) VALUES (1, 'committed')").Error
	}))
	cause := errors.New("rollback requested")
	err = client.WithinTx(ctx, func(txCtx context.Context) error {
		txDB, txErr := client.DB(txCtx)
		if txErr != nil {
			return txErr
		}
		if txErr = txDB.Exec("INSERT INTO items(id, name) VALUES (2, 'rolled back')").Error; txErr != nil {
			return txErr
		}
		return cause
	})
	assert.ErrorIs(t, err, cause)

	assert.PanicsWithValue(t, "panic rollback", func() {
		_ = client.WithinTx(ctx, func(txCtx context.Context) error {
			txDB, txErr := client.DB(txCtx)
			require.NoError(t, txErr)
			require.NoError(t, txDB.Exec("INSERT INTO items(id, name) VALUES (3, 'panic')").Error)
			panic("panic rollback")
		})
	})

	require.NoError(t, client.WithinTx(ctx, func(txCtx context.Context) error {
		innerErr := client.WithinTx(txCtx, func(context.Context) error { return errors.New("handled") })
		assert.Error(t, innerErr)
		txDB, txErr := client.DB(txCtx)
		if txErr != nil {
			return txErr
		}
		return txDB.Exec("INSERT INTO items(id, name) VALUES (4, 'nested')").Error
	}))

	var count int64
	require.NoError(t, db.Raw("SELECT COUNT(*) FROM items").Scan(&count).Error)
	assert.EqualValues(t, 2, count)
	err = db.Exec("INSERT INTO items(id, name) VALUES (1, 'duplicate')").Error
	assert.ErrorIs(t, err, gorm.ErrDuplicatedKey)
	assert.Contains(t, logs.String(), `"level":"ERROR"`)
	assert.NotContains(t, logs.String(), "duplicate in transaction")
	assert.NotContains(t, logs.String(), "postgres://")

	err = client.WithinTx(ctx, func(txCtx context.Context) error {
		txDB, txErr := client.DB(txCtx)
		if txErr != nil {
			return txErr
		}
		_ = txDB.Exec("INSERT INTO items(id, name) VALUES (1, 'duplicate in transaction')").Error
		return nil
	})
	assert.Error(t, err, "PostgreSQL must reject commit after an aborted statement")
}
