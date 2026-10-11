package kafka

import (
	"context"
	"crypto/tls"
	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	base := Config{Brokers: []string{"localhost:9092"}, ClientID: "worker"}
	require.NoError(t, base.Validate())
	defaults := base.defaults()
	assert.Equal(t, 5*time.Second, defaults.DialTimeout)
	assert.Equal(t, 10*time.Second, defaults.PublishTimeout)
	assert.Equal(t, 10*time.Second, defaults.HandlerTimeout)
	assert.Equal(t, 45*time.Second, defaults.PollProcessingTimeout)
	assert.Equal(t, 60*time.Second, defaults.RebalanceTimeout)
	assert.Equal(t, 30*time.Second, defaults.ShutdownTimeout)
	assert.Equal(t, 32, defaults.MaxPollRecords)
	cases := []struct {
		name   string
		change func(*Config)
	}{{"brokers", func(c *Config) { c.Brokers = nil }}, {"client", func(c *Config) { c.ClientID = "" }}, {"group", func(c *Config) { c.ConsumerEnabled = true }}, {"margin", func(c *Config) { c.RebalanceTimeout = 40 * time.Second }}, {"SASL", func(c *Config) { c.SASL = &SASLConfig{Mechanism: "PLAIN", Username: "secret"} }}, {"DLQ", func(c *Config) { c.DLQTopic = "invalid" }}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { c := base; tc.change(&c); assert.ErrorIs(t, c.Validate(), em.ErrInvalidConfig) })
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	base.TLS = tlsConfig
	assert.NotSame(t, tlsConfig, base.defaults().TLS)
}
func TestNewFailsBeforeNetwork(t *testing.T) {
	c := Config{Brokers: []string{"unreachable:1"}, ClientID: "worker", ConsumerEnabled: true}
	_, err := New(context.Background(), c)
	assert.ErrorIs(t, err, em.ErrInvalidConfig)
}
func TestConfigSecurityOptions(t *testing.T) {
	base := Config{Brokers: []string{"localhost:9092"}, ClientID: "worker"}
	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		c := base
		c.SASL = &SASLConfig{Mechanism: mech, Username: "user", Password: "very-secret"}
		require.NoError(t, c.Validate())
		assert.NotEmpty(t, clientOptions(c))
	}
	bad := base
	bad.SASL = &SASLConfig{Mechanism: "UNKNOWN", Username: "u", Password: "very-secret"}
	assert.ErrorIs(t, bad.Validate(), em.ErrInvalidConfig)
	tlsC := base
	tlsC.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	assert.NotEmpty(t, clientOptions(tlsC))
	for _, tc := range []Config{{Brokers: []string{""}, ClientID: "c"}, {Brokers: base.Brokers, ClientID: "c", MaxPollRecords: 1001}, {Brokers: base.Brokers, ClientID: "c", PublishTimeout: -time.Second}} {
		assert.ErrorIs(t, tc.Validate(), em.ErrInvalidConfig)
	}
}

func TestConfigDoesNotReadEnvironment(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "malformed")
	t.Setenv("KAFKA_GROUP_ID", "unexpected")
	cfg := Config{Brokers: []string{"localhost:9092"}, ClientID: "worker"}
	require.NoError(t, cfg.Validate())
	assert.Empty(t, cfg.GroupID)
}
