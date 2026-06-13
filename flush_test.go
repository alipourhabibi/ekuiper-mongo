package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

func TestFlush_EmptyBuffer(t *testing.T) {
	ins := &mockInserter{}
	m := newSinkWithMock(newDefaultConfig(), ins)
	m.flush(context.Background())
	if ins.Calls() != 0 {
		t.Error("InsertMany should not be called for empty buffer")
	}
}

func TestFlush_BasicInsert(t *testing.T) {
	ins := &mockInserter{}
	m := newSinkWithMock(newDefaultConfig(), ins)
	m.buffer = []interface{}{[]byte(`[{"field":"value"}]`)}
	m.flush(context.Background())
	if ins.Calls() != 1 {
		t.Errorf("expected 1 InsertMany call, got %d", ins.Calls())
	}
	if len(ins.lastDocs) != 1 {
		t.Errorf("expected 1 doc, got %d", len(ins.lastDocs))
	}
}

func TestFlush_BufferDrainedAfterFlush(t *testing.T) {
	ins := &mockInserter{}
	m := newSinkWithMock(newDefaultConfig(), ins)
	m.buffer = []interface{}{[]byte(`[{"x":1}]`)}
	m.flush(context.Background())
	m.mu.Lock()
	remaining := len(m.buffer)
	m.mu.Unlock()
	if remaining != 0 {
		t.Errorf("buffer should be empty after flush, got %d items", remaining)
	}
}

func TestFlush_RetryOnTransientError(t *testing.T) {
	ins := &mockInserter{failFor: 2}
	cfg := newDefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 1 * time.Millisecond
	m := newSinkWithMock(cfg, ins)
	m.buffer = []interface{}{[]byte(`[{"x":1}]`)}
	m.flush(context.Background())
	if ins.Calls() != 3 {
		t.Errorf("expected 3 InsertMany calls (2 failures + 1 success), got %d", ins.Calls())
	}
}

func TestFlush_NoRetryOnDuplicateKey(t *testing.T) {
	dupErr := mongo.WriteException{
		WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key error"}},
	}
	ins := &mockInserter{failFor: 99, failWith: dupErr}
	cfg := newDefaultConfig()
	cfg.MaxRetries = 5
	cfg.RetryBaseDelay = 1 * time.Millisecond
	m := newSinkWithMock(cfg, ins)
	m.buffer = []interface{}{[]byte(`[{"x":1}]`)}
	m.flush(context.Background())
	if ins.Calls() != 1 {
		t.Errorf("duplicate key error: expected 1 call (no retry), got %d", ins.Calls())
	}
}

func TestFlush_ExhaustedRetries(t *testing.T) {
	ins := &mockInserter{failFor: 99}
	cfg := newDefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryBaseDelay = 1 * time.Millisecond
	m := newSinkWithMock(cfg, ins)
	m.buffer = []interface{}{[]byte(`[{"x":1}]`)}
	m.flush(context.Background())
	if ins.Calls() != 3 {
		t.Errorf("expected exactly 3 calls on exhaustion, got %d", ins.Calls())
	}
}

// TestFlush_DrainRace verifies no data race between concurrent Collect and flush.
// Run with: go test -race
func TestFlush_DrainRace(t *testing.T) {
	ins := &mockInserter{}
	cfg := newDefaultConfig()
	cfg.RetryBaseDelay = 1 * time.Millisecond
	m := newSinkWithMock(cfg, ins)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Collect(nil, []byte(`[{"race":"test"}]`))
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.flush(context.Background())
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestFlushLoop_FinalFlushOnClose(t *testing.T) {
	var total int64
	ins := &mockInserter{}
	cfg := newDefaultConfig()
	cfg.Interval = 10 * time.Millisecond
	cfg.RetryBaseDelay = 1 * time.Millisecond
	m := newSinkWithMock(cfg, ins)
	m.colFunc = func() inserter {
		return &countingInserter{delegate: ins, total: &total}
	}

	ctx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(cfg.Interval)
	m.wg.Add(1)
	go m.flushLoop(ctx, ticker)

	for i := 0; i < 5; i++ {
		m.buffer = append(m.buffer, []byte(fmt.Sprintf(`[{"i":%d}]`, i)))
	}

	cancel()
	m.wg.Wait()

	if atomic.LoadInt64(&total) < 5 {
		t.Errorf("expected at least 5 docs in final flush, got %d", total)
	}
}

func TestFlushLoop_TickerStopped(t *testing.T) {
	cfg := newDefaultConfig()
	cfg.Interval = 10 * time.Millisecond
	m := newSinkWithMock(cfg, &mockInserter{})

	ctx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(cfg.Interval)
	m.wg.Add(1)
	go m.flushLoop(ctx, ticker)
	cancel()
	m.wg.Wait()
}
