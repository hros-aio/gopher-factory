package storesql

import (
	"context"
	"log/slog"
	"time"

	gormlogger "gorm.io/gorm/logger"
)

type gormSlogLogger struct {
	logger    *slog.Logger
	threshold time.Duration
	level     gormlogger.LogLevel
}

func newGormLogger(logger *slog.Logger, threshold time.Duration) *gormSlogLogger {
	return &gormSlogLogger{logger: logger, threshold: threshold, level: gormlogger.Info}
}

func (l *gormSlogLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	clone := *l
	clone.level = level
	return &clone
}

func (l *gormSlogLogger) Info(ctx context.Context, _ string, _ ...interface{}) {
	if l.level >= gormlogger.Info {
		l.logger.InfoContext(ctx, "gorm info")
	}
}

func (l *gormSlogLogger) Warn(ctx context.Context, _ string, _ ...interface{}) {
	if l.level >= gormlogger.Warn {
		l.logger.WarnContext(ctx, "gorm warning")
	}
}

func (l *gormSlogLogger) Error(ctx context.Context, _ string, _ ...interface{}) {
	if l.level >= gormlogger.Error {
		l.logger.ErrorContext(ctx, "gorm error")
	}
}

func (l *gormSlogLogger) Trace(ctx context.Context, begin time.Time, _ func() (string, int64), err error) {
	if l.level == gormlogger.Silent {
		return
	}
	elapsed := time.Since(begin)
	attrs := []slog.Attr{slog.Duration("elapsed", elapsed)}
	if err != nil && l.level >= gormlogger.Error {
		attrs = append(attrs, slog.String("error_class", errorClass(err)))
		l.logger.LogAttrs(ctx, slog.LevelError, "gorm query failed", attrs...)
		return
	}
	if elapsed > l.threshold && l.level >= gormlogger.Warn {
		l.logger.LogAttrs(ctx, slog.LevelWarn, "gorm slow query", attrs...)
	}
}

func (l *gormSlogLogger) ParamsFilter(_ context.Context, sql string, _ ...interface{}) (string, []interface{}) {
	return sql, nil
}
