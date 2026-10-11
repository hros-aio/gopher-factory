package storesql

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"gorm.io/gorm"
)

type options struct {
	logger  *slog.Logger
	plugins []gorm.Plugin
}

// Option customizes a Client during construction.
type Option func(*options) error

func defaultOptions() options { return options{logger: slog.Default()} }

func applyOptions(cfg *options, opts []Option) error {
	for _, opt := range opts {
		if opt == nil {
			return errors.New("store-sql option is nil")
		}
		if err := opt(cfg); err != nil {
			return fmt.Errorf("apply store-sql option: %w", err)
		}
	}
	return nil
}

// WithLogger routes GORM diagnostics to logger. A nil logger is invalid.
func WithLogger(logger *slog.Logger) Option {
	return func(cfg *options) error {
		if logger == nil {
			return errors.New("store-sql logger is nil")
		}
		cfg.logger = logger
		return nil
	}
}

// WithPlugin registers a GORM plugin on the Client's database handle.
func WithPlugin(plugin gorm.Plugin) Option {
	return func(cfg *options) error {
		if plugin == nil {
			return errors.New("store-sql plugin is nil")
		}
		value := reflect.ValueOf(plugin)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			if value.IsNil() {
				return errors.New("store-sql plugin is nil")
			}
		}
		cfg.plugins = append(cfg.plugins, plugin)
		return nil
	}
}
