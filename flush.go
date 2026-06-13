package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// flushLoop runs until ctx is cancelled, then performs one final flush.
func (m *mongoSink) flushLoop(ctx context.Context, t *time.Ticker) {
	defer m.wg.Done()
	defer t.Stop()
	for {
		select {
		case <-t.C:
			m.flush(ctx)
		case <-ctx.Done():
			// Use a fresh context for the final flush: execCtx is already cancelled.
			m.flush(context.Background())
			return
		}
	}
}

// flush atomically drains the buffer then inserts to MongoDB without holding the lock.
//
// Copy-drain-unlock pattern:
//  1. Lock → snapshot buffer → reset buffer pointer → Unlock
//  2. Normalise items (no lock held)
//  3. InsertMany with retry (no lock held)
func (m *mongoSink) flush(ctx context.Context) {
	m.mu.Lock()
	if len(m.buffer) == 0 {
		m.mu.Unlock()
		return
	}
	batch := m.buffer
	m.buffer = make([]interface{}, 0, cap(batch))
	m.mu.Unlock()

	var docs []interface{}
	for _, item := range batch {
		normalized, err := normaliseItem(item)
		if err != nil {
			m.log.Errorf("flush: skip malformed item: %v", err)
			continue
		}
		docs = append(docs, normalized...)
	}
	if len(docs) == 0 {
		return
	}

	if err := m.insertWithRetry(ctx, docs); err != nil {
		m.log.Errorf("flush: insert failed: %v", err)
	}
}

// insertWithRetry calls InsertMany up to cfg.MaxRetries times with exponential backoff.
// Non-transient errors (e.g. duplicate key) abort immediately.
func (m *mongoSink) insertWithRetry(ctx context.Context, docs []interface{}) error {
	col := m.colFunc()
	var lastErr error
	for attempt := 0; attempt < m.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(float64(m.cfg.RetryBaseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			m.log.Warnf("insertWithRetry: attempt %d/%d, backing off %v: %v",
				attempt+1, m.cfg.MaxRetries, delay, lastErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry backoff: %w", ctx.Err())
			}
		}
		_, err := col.InsertMany(ctx, docs)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientError(err) {
			return fmt.Errorf("non-transient error, aborting: %w", err)
		}
	}
	return fmt.Errorf("exhausted %d attempts: %w", m.cfg.MaxRetries, lastErr)
}

// isTransientError returns false for errors that should not be retried.
func isTransientError(err error) bool {
	return !mongo.IsDuplicateKeyError(err)
}
