package storesql

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gormlogger "gorm.io/gorm/logger"
)

func TestGormLoggerRedactsError(t *testing.T) {
	var output bytes.Buffer
	logger := newGormLogger(slog.New(slog.NewJSONHandler(&output, nil)), time.Millisecond)
	interpolated := false
	logger.Trace(context.Background(), time.Now().Add(-time.Second), func() (string, int64) {
		interpolated = true
		return "INSERT INTO private VALUES ('bound-secret')", 1
	}, errors.New("password=dsn-secret"))
	record := output.String()
	require.NotEmpty(t, record)
	assert.False(t, interpolated)
	assert.Contains(t, record, `"level":"ERROR"`)
	assert.NotContains(t, record, "bound-secret")
	assert.NotContains(t, record, "dsn-secret")
	assert.NotContains(t, record, "INSERT")
}

func TestGormLoggerSlowThreshold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold time.Duration
		warn      bool
	}{
		{"slow", time.Millisecond, true},
		{"within threshold", time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := newGormLogger(slog.New(slog.NewJSONHandler(&output, nil)), tc.threshold)
			logger.Trace(context.Background(), time.Now().Add(-time.Second), func() (string, int64) {
				return "SELECT 'pii-secret'", 3
			}, nil)
			assert.Equal(t, tc.warn, bytes.Contains(output.Bytes(), []byte(`"level":"WARN"`)))
			assert.NotContains(t, output.String(), "pii-secret")
		})
	}
}

func TestGormLoggerGenericMethodsAndFilter(t *testing.T) {
	var output bytes.Buffer
	logger := newGormLogger(slog.New(slog.NewJSONHandler(&output, nil)), time.Second)
	logger.Info(context.Background(), "secret=%s", "value")
	logger.Warn(context.Background(), "secret=%s", "value")
	logger.Error(context.Background(), "secret=%s", "value")
	assert.NotContains(t, output.String(), "value")
	_, params := logger.ParamsFilter(context.Background(), "SELECT ?", "value")
	assert.Nil(t, params)
	muted := logger.LogMode(gormlogger.Silent)
	muted.Trace(context.Background(), time.Now().Add(-time.Second), nil, nil)
}
