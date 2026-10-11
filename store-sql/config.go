package storesql

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Config describes one PostgreSQL pool and its slow-query threshold.
type Config struct {
	DSN                string
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
	ConnMaxIdleTime    time.Duration
	SlowQueryThreshold time.Duration
}

// Validate fills omitted defaults and rejects invalid settings without network I/O.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: Config is nil", ErrInvalidConfig)
	}
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}
	if _, err := pgx.ParseConfig(c.DSN); err != nil {
		return fmt.Errorf("%w: DSN is invalid", ErrInvalidConfig)
	}
	if c.MaxOpenConns < 0 {
		return fmt.Errorf("%w: MaxOpenConns must be positive", ErrInvalidConfig)
	}
	if c.MaxIdleConns < 0 {
		return fmt.Errorf("%w: MaxIdleConns must be nonnegative", ErrInvalidConfig)
	}
	if c.ConnMaxLifetime < 0 {
		return fmt.Errorf("%w: ConnMaxLifetime must be positive", ErrInvalidConfig)
	}
	if c.ConnMaxIdleTime < 0 {
		return fmt.Errorf("%w: ConnMaxIdleTime must be positive", ErrInvalidConfig)
	}
	if c.SlowQueryThreshold < 0 {
		return fmt.Errorf("%w: SlowQueryThreshold must be positive", ErrInvalidConfig)
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 10
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = min(5, c.MaxOpenConns)
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		return fmt.Errorf("%w: MaxIdleConns must not exceed MaxOpenConns", ErrInvalidConfig)
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 30 * time.Minute
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = 5 * time.Minute
	}
	if c.SlowQueryThreshold == 0 {
		c.SlowQueryThreshold = 200 * time.Millisecond
	}
	return nil
}
