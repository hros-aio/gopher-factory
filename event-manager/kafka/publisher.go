package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/twmb/franz-go/pkg/kgo"
)

type clientState struct {
	mu                            sync.Mutex
	handlers                      map[string]em.Handler
	consumer                      consumerAPI
	running, ran, closing, closed bool
	closeDone, runDone            chan struct{}
	closeErr                      error
	runCancel                     context.CancelFunc
	shutdownAt                    time.Time
	pubWG                         sync.WaitGroup
}

type producerAPI interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Ping(context.Context) error
	Flush(context.Context) error
	Close()
}

// Publish prepares a copy and waits for all in-sync replicas to acknowledge it.
func (c *Client) Publish(ctx context.Context, topic, key string, event em.Event) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", em.ErrPublish, err)
	}
	c.state.mu.Lock()
	if c.state.closing {
		c.state.mu.Unlock()
		return em.ErrClosed
	}
	c.state.pubWG.Add(1)
	c.state.mu.Unlock()
	defer c.state.pubWG.Done()
	if err := em.ValidateTopic(topic); err != nil {
		return err
	}
	e, err := em.PrepareEvent(ctx, event, c.cfg.ClientID)
	if err != nil {
		return err
	}
	b, err := encodeEvent(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.PublishTimeout)
	defer cancel()
	_, err = c.producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: b}).First()
	if err != nil {
		return fmt.Errorf("%w: %w", em.ErrPublish, err)
	}
	return nil
}

// Ping checks live broker reachability and reports closure distinctly.
func (c *Client) Ping(ctx context.Context) error {
	c.state.mu.Lock()
	closed := c.state.closing
	c.state.mu.Unlock()
	if closed {
		return em.ErrClosed
	}
	if err := c.producer.Ping(ctx); err != nil {
		return fmt.Errorf("%w: ping: %w", em.ErrPublish, err)
	}
	return nil
}

// Close stops consumption, flushes pending records, and closes owned clients.
func (c *Client) Close(ctx context.Context) error {
	c.state.mu.Lock()
	if !c.state.closing {
		c.state.closing = true
		c.state.closeDone = make(chan struct{})
		if c.state.runCancel != nil {
			c.state.runCancel()
		}
		go c.closeClients()
	}
	done := c.state.closeDone
	c.state.mu.Unlock()
	select {
	case <-done:
		c.state.mu.Lock()
		err := c.state.closeErr
		c.state.mu.Unlock()
		return err
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", em.ErrShutdownTimeout, ctx.Err())
	}
}

func (c *Client) closeClients() {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ShutdownTimeout)
	defer cancel()
	c.state.mu.Lock()
	runDone := c.state.runDone
	c.state.mu.Unlock()
	if runDone != nil {
		select {
		case <-runDone:
		case <-ctx.Done():
		}
	}
	pubDone := make(chan struct{})
	go func() { c.state.pubWG.Wait(); close(pubDone) }()
	select {
	case <-pubDone:
	case <-ctx.Done():
	}
	err := c.producer.Flush(ctx)
	c.state.mu.Lock()
	consumer := c.state.consumer
	c.state.mu.Unlock()
	if consumer != nil {
		consumer.CloseAllowingRebalance()
	}
	c.producer.Close()
	c.state.mu.Lock()
	if err != nil {
		c.state.closeErr = fmt.Errorf("%w: flush: %w", em.ErrPublish, err)
	}
	c.state.closed = true
	close(c.state.closeDone)
	c.state.mu.Unlock()
}

var _ em.Publisher = (*Client)(nil)
