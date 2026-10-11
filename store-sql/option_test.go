package storesql

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type testPlugin struct {
	err error
}

func (p testPlugin) Name() string              { return "test-plugin" }
func (p testPlugin) Initialize(*gorm.DB) error { return p.err }

func TestOptions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	assert.Same(t, slog.Default(), defaultOptions().logger)
	var typedNil *testPlugin
	for _, tc := range []struct {
		name    string
		opt     Option
		wantErr bool
	}{
		{"logger", WithLogger(logger), false},
		{"plugin", WithPlugin(testPlugin{}), false},
		{"nil logger", WithLogger(nil), true},
		{"nil plugin", WithPlugin(nil), true},
		{"typed nil plugin", WithPlugin(typedNil), true},
		{"nil option", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultOptions()
			err := applyOptions(&cfg, []Option{tc.opt})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.name == "logger" {
				assert.Same(t, logger, cfg.logger)
			}
			if tc.name == "plugin" {
				assert.Len(t, cfg.plugins, 1)
			}
		})
	}
}

func TestPluginErrorFixture(t *testing.T) {
	cause := errors.New("registration failed")
	assert.ErrorIs(t, testPlugin{err: cause}.Initialize(nil), cause)
}
