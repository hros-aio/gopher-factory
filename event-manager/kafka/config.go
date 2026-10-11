package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SASLConfig supplies credentials directly; they are never logged.
type SASLConfig struct{ Mechanism, Username, Password string }

// Config defines one Kafka connection. ConsumerEnabled requires GroupID before dialing.
type Config struct {
	Brokers               []string
	ClientID              string
	GroupID               string
	ConsumerEnabled       bool
	TLS                   *tls.Config
	SASL                  *SASLConfig
	DialTimeout           time.Duration
	PublishTimeout        time.Duration
	HandlerTimeout        time.Duration
	PollProcessingTimeout time.Duration
	RebalanceTimeout      time.Duration
	ShutdownTimeout       time.Duration
	MaxPollRecords        int
	DLQTopic              string
}

func (c Config) defaults() Config {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.HandlerTimeout == 0 {
		c.HandlerTimeout = 10 * time.Second
	}
	if c.PollProcessingTimeout == 0 {
		c.PollProcessingTimeout = 45 * time.Second
	}
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = 60 * time.Second
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.MaxPollRecords == 0 {
		c.MaxPollRecords = 32
	}
	if c.TLS != nil {
		c.TLS = c.TLS.Clone()
	}
	return c
}

// Validate performs field-specific validation without network I/O.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("%w: brokers required", em.ErrInvalidConfig)
	}
	for _, b := range c.Brokers {
		if b == "" {
			return fmt.Errorf("%w: empty broker", em.ErrInvalidConfig)
		}
	}
	if c.ClientID == "" {
		return fmt.Errorf("%w: client id required", em.ErrInvalidConfig)
	}
	if c.ConsumerEnabled && c.GroupID == "" {
		return fmt.Errorf("%w: group id required when consumer enabled", em.ErrInvalidConfig)
	}
	if c.DialTimeout < 0 || c.PublishTimeout < 0 || c.HandlerTimeout < 0 || c.PollProcessingTimeout < 0 || c.RebalanceTimeout < 0 || c.ShutdownTimeout < 0 || c.MaxPollRecords < 0 {
		return fmt.Errorf("%w: negative timeout or max poll records", em.ErrInvalidConfig)
	}
	c = c.defaults()
	if c.MaxPollRecords > 1000 {
		return fmt.Errorf("%w: max poll records exceeds 1000", em.ErrInvalidConfig)
	}
	if c.PollProcessingTimeout+10*time.Second > c.RebalanceTimeout {
		return fmt.Errorf("%w: poll budget must leave ten seconds before rebalance timeout", em.ErrInvalidConfig)
	}
	if c.SASL != nil {
		if c.SASL.Username == "" || c.SASL.Password == "" {
			return fmt.Errorf("%w: SASL username and password required", em.ErrInvalidConfig)
		}
		switch c.SASL.Mechanism {
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		default:
			return fmt.Errorf("%w: unsupported SASL mechanism", em.ErrInvalidConfig)
		}
	}
	if c.DLQTopic != "" {
		if err := em.ValidateTopic(c.DLQTopic); err != nil {
			return err
		}
	}
	return nil
}

type options struct {
	logger      *slog.Logger
	newClient   func(...kgo.Opt) (*kgo.Client, error)
	newConsumer func(...kgo.Opt) (consumerAPI, error)
}

// Option customizes a client.
type Option func(*options)

// WithLogger installs a structured logger.
func WithLogger(logger *slog.Logger) Option { return func(o *options) { o.logger = logger } }

// Client implements the root Publisher and Subscriber interfaces.
type Client struct {
	cfg      Config
	opts     options
	producer producerAPI
	state    clientState
}

// New validates configuration before dialing, then verifies broker reachability.
func New(ctx context.Context, cfg Config, opt ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.defaults()
	o := options{logger: slog.Default(), newClient: kgo.NewClient}
	o.newConsumer = func(opts ...kgo.Opt) (consumerAPI, error) { return o.newClient(opts...) }
	for _, f := range opt {
		f(&o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	kopts := clientOptions(cfg)
	kopts = append(kopts, kgo.RequiredAcks(kgo.AllISRAcks()))
	p, err := o.newClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("%w: client: %w", em.ErrInvalidConfig, err)
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("%w: broker ping: %w", em.ErrPublish, err)
	}
	return &Client{cfg: cfg, opts: o, producer: p, state: clientState{handlers: make(map[string]em.Handler)}}, nil
}

func clientOptions(c Config) []kgo.Opt {
	o := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.ClientID), kgo.DialTimeout(c.DialTimeout)}
	if c.TLS != nil {
		o = append(o, kgo.DialTLSConfig(c.TLS.Clone()))
	}
	if c.SASL != nil {
		switch c.SASL.Mechanism {
		case "PLAIN":
			o = append(o, kgo.SASL(plain.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsMechanism()))
		case "SCRAM-SHA-256":
			o = append(o, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			o = append(o, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha512Mechanism()))
		}
	}
	return o
}
