package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	em "github.com/hros-aio/gopher-factory/event-manager"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Register installs one handler for an exact source topic before Run.
func (c *Client) Register(topic string, h em.Handler) error {
	if err := em.ValidateTopic(topic); err != nil {
		return err
	}
	if h == nil {
		return fmt.Errorf("%w: nil handler", em.ErrInvalidConfig)
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.ran || c.state.closing {
		return em.ErrState
	}
	if !c.cfg.ConsumerEnabled {
		return fmt.Errorf("%w: consumer mode disabled", em.ErrInvalidConfig)
	}
	if topic == c.cfg.DLQTopic {
		return fmt.Errorf("%w: DLQ equals source topic", em.ErrInvalidConfig)
	}
	if _, ok := c.state.handlers[topic]; ok {
		return fmt.Errorf("%w: duplicate topic", em.ErrState)
	}
	c.state.handlers[topic] = h
	return nil
}

type partitionKey struct {
	topic     string
	partition int32
}
type outcome struct {
	key    partitionKey
	record *kgo.Record
	err    error
	done   bool
}
type consumerAPI interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	AllowRebalance()
	CloseAllowingRebalance()
	Ping(context.Context) error
}

// Run processes one poll at a time, sequentially per partition and in parallel across partitions.
func (c *Client) Run(ctx context.Context) (retErr error) {
	c.state.mu.Lock()
	if c.state.ran || c.state.closing {
		c.state.mu.Unlock()
		return em.ErrState
	}
	if !c.cfg.ConsumerEnabled || c.cfg.GroupID == "" {
		c.state.mu.Unlock()
		return fmt.Errorf("%w: group id required", em.ErrInvalidConfig)
	}
	if len(c.state.handlers) == 0 {
		c.state.mu.Unlock()
		return fmt.Errorf("%w: no handlers", em.ErrInvalidConfig)
	}
	topics := make([]string, 0, len(c.state.handlers))
	for t := range c.state.handlers {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	runCtx, cancel := context.WithCancel(ctx)
	c.state.runCancel = cancel
	c.state.ran = true
	c.state.running = true
	c.state.runDone = make(chan struct{})
	c.state.mu.Unlock()
	defer func() {
		cancel()
		c.state.mu.Lock()
		c.state.running = false
		close(c.state.runDone)
		if c.state.shutdownAt.IsZero() {
			c.state.shutdownAt = time.Now()
		}
		deadline := c.state.shutdownAt.Add(c.cfg.ShutdownTimeout)
		c.state.mu.Unlock()
		closeCtx, stop := context.WithDeadline(context.Background(), deadline)
		defer stop()
		if err := c.Close(closeCtx); err != nil && retErr == nil {
			retErr = err
		}
	}()
	opts := clientOptions(c.cfg)
	opts = append(opts, kgo.ConsumerGroup(c.cfg.GroupID), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.RebalanceTimeout(c.cfg.RebalanceTimeout))
	consumer, err := c.opts.newConsumer(opts...)
	if err != nil {
		return fmt.Errorf("%w: consumer client: %w", em.ErrInvalidConfig, err)
	}
	c.state.mu.Lock()
	c.state.consumer = consumer
	c.state.mu.Unlock()
	if err = consumer.Ping(runCtx); err != nil {
		return fmt.Errorf("%w: consumer ping: %w", em.ErrProcess, err)
	}
	for {
		if runCtx.Err() != nil {
			c.markShutdown()
			return nil
		}
		fetches := consumer.PollRecords(runCtx, c.cfg.MaxPollRecords)
		if runCtx.Err() != nil {
			c.markShutdown()
			consumer.AllowRebalance()
			return nil
		}
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, e error) {
			if fetchErr == nil {
				fetchErr = e
			}
		})
		if fetchErr != nil {
			consumer.AllowRebalance()
			return fmt.Errorf("%w: poll: %w", em.ErrProcess, fetchErr)
		}
		groups := make(map[partitionKey][]*kgo.Record)
		total := 0
		fetches.EachRecord(func(r *kgo.Record) {
			k := partitionKey{r.Topic, r.Partition}
			groups[k] = append(groups[k], r)
			total++
		})
		if total == 0 {
			consumer.AllowRebalance()
			continue
		}
		if err := c.processBatch(runCtx, consumer, groups, total); err != nil {
			return err
		}
	}
}

func (c *Client) processBatch(runCtx context.Context, consumer consumerAPI, groups map[partitionKey][]*kgo.Record, total int) (retErr error) {
	// The deadline covers handlers, commits, and barrier release; it never exceeds the poll budget.
	batchCtx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), c.cfg.PollProcessingTimeout)
	defer cancel()
	results := make(chan outcome, total+len(groups))
	var wg sync.WaitGroup
	for key, records := range groups {
		wg.Add(1)
		go func(key partitionKey, records []*kgo.Record) {
			defer wg.Done()
			for _, r := range records {
				if runCtx.Err() != nil || batchCtx.Err() != nil {
					break
				}
				m, err := decodeRecord(r)
				if err != nil {
					if c.cfg.DLQTopic != "" {
						err = em.PublishDeadLetter(batchCtx, c, c.cfg.DLQTopic, m, err)
					}
				}
				if err == nil && m.Envelope != nil {
					hctx, stop := context.WithTimeout(batchCtx, c.cfg.HandlerTimeout)
					err = c.state.handlers[key.topic](hctx, m)
					stop()
				}
				if err != nil {
					results <- outcome{key: key, err: fmt.Errorf("%w: %s/%d/%d: %w", em.ErrProcess, r.Topic, r.Partition, r.Offset, err), done: true}
					return
				}
				results <- outcome{key: key, record: r}
			}
			results <- outcome{key: key, done: true}
		}(key, records)
	}
	go func() { wg.Wait(); close(results) }()
	frontiers := make(map[partitionKey]*kgo.Record)
	done := 0
	var firstErr error
	for done < len(groups) {
		select {
		case v, ok := <-results:
			if !ok {
				done = len(groups)
				break
			}
			if v.record != nil {
				frontiers[v.key] = v.record
			}
			if v.done {
				done++
				if v.err != nil && firstErr == nil {
					firstErr = v.err
				}
			}
		case <-runCtx.Done():
			// Draining uses the remaining poll budget and at most ShutdownTimeout.
			c.markShutdown()
			c.state.mu.Lock()
			remaining := time.Until(c.state.shutdownAt.Add(c.cfg.ShutdownTimeout))
			c.state.mu.Unlock()
			budget := min(remaining, time.Until(deadlineOf(batchCtx)))
			if budget < 0 {
				budget = 0
			}
			stopAllowance := min(250*time.Millisecond, budget/4)
			grace := time.NewTimer(budget - stopAllowance)
			defer grace.Stop()
			draining := true
			for draining {
				select {
				case v, ok := <-results:
					if !ok {
						draining = false
						break
					}
					if v.record != nil {
						frontiers[v.key] = v.record
					}
					if v.done {
						done++
						if v.err != nil && firstErr == nil {
							firstErr = v.err
						}
					}
					if done == len(groups) {
						draining = false
					}
				case <-grace.C:
					draining = false
					cancel()
				}
			}
			if done < len(groups) {
				settle := time.NewTimer(stopAllowance)
				settling := true
				for settling {
					select {
					case v, ok := <-results:
						if !ok {
							settling = false
							break
						}
						if v.record != nil {
							frontiers[v.key] = v.record
						}
						if v.done {
							done++
							if v.err != nil && firstErr == nil {
								firstErr = v.err
							}
						}
						settling = done < len(groups)
					case <-settle.C:
						settling = false
					}
				}
				settle.Stop()
				if done < len(groups) {
					firstErr = errors.Join(firstErr, em.ErrShutdownTimeout)
				}
			}
			done = len(groups)
		case <-batchCtx.Done():
			cancel()
			firstErr = errors.Join(firstErr, em.ErrShutdownTimeout)
			settle := time.NewTimer(250 * time.Millisecond)
			settling := true
			for settling {
				select {
				case v, ok := <-results:
					if !ok {
						settling = false
						break
					}
					if v.record != nil {
						frontiers[v.key] = v.record
					}
					if v.done {
						done++
					}
					settling = done < len(groups)
				case <-settle.C:
					settling = false
				}
			}
			settle.Stop()
			done = len(groups)
		}
	}
	// Capture already completed outcomes before committing, even at a deadline.
	for {
		select {
		case v, ok := <-results:
			if !ok {
				goto commit
			}
			if v.record != nil {
				frontiers[v.key] = v.record
			}
			if v.err != nil && firstErr == nil {
				firstErr = v.err
			}
		default:
			goto commit
		}
	}
commit:
	commitCtx := batchCtx
	if batchCtx.Err() != nil {
		var stop context.CancelFunc
		commitCtx, stop = context.WithTimeout(context.Background(), min(2*time.Second, c.cfg.RebalanceTimeout/10))
		defer stop()
	}
	if len(frontiers) != 0 {
		commits := make([]*kgo.Record, 0, len(frontiers))
		for _, r := range frontiers {
			commits = append(commits, r)
		}
		if err := consumer.CommitRecords(commitCtx, commits...); err != nil {
			firstErr = errors.Join(firstErr, fmt.Errorf("%w: %w", em.ErrCommit, err))
		}
	}
	consumer.AllowRebalance()
	return firstErr
}

func deadlineOf(ctx context.Context) time.Time { d, _ := ctx.Deadline(); return d }
func (c *Client) markShutdown() {
	c.state.mu.Lock()
	if c.state.shutdownAt.IsZero() {
		c.state.shutdownAt = time.Now()
	}
	c.state.mu.Unlock()
}

var _ em.Subscriber = (*Client)(nil)
