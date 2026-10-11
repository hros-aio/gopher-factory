package storesql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDSN = "postgres://worker:secret@localhost:5432/worker?sslmode=disable"

func TestConfigValidateDefaults(t *testing.T) {
	cfg := Config{DSN: testDSN}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 10, cfg.MaxOpenConns)
	assert.Equal(t, 5, cfg.MaxIdleConns)
	assert.Equal(t, 30*time.Minute, cfg.ConnMaxLifetime)
	assert.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime)
	assert.Equal(t, 200*time.Millisecond, cfg.SlowQueryThreshold)
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidateSmallPool(t *testing.T) {
	cfg := Config{DSN: testDSN, MaxOpenConns: 1}
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 1, cfg.MaxIdleConns)
}

func TestConfigValidateInvalid(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cfg   Config
		field string
	}{
		{"missing DSN", Config{}, "DSN"},
		{"malformed DSN", Config{DSN: "postgres://%"}, "DSN"},
		{"negative open", Config{DSN: testDSN, MaxOpenConns: -1}, "MaxOpenConns"},
		{"negative idle", Config{DSN: testDSN, MaxIdleConns: -1}, "MaxIdleConns"},
		{"idle over open", Config{DSN: testDSN, MaxOpenConns: 1, MaxIdleConns: 2}, "MaxIdleConns"},
		{"negative lifetime", Config{DSN: testDSN, ConnMaxLifetime: -1}, "ConnMaxLifetime"},
		{"negative idle time", Config{DSN: testDSN, ConnMaxIdleTime: -1}, "ConnMaxIdleTime"},
		{"negative threshold", Config{DSN: testDSN, SlowQueryThreshold: -1}, "SlowQueryThreshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidConfig)
			assert.ErrorContains(t, err, tc.field)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}
