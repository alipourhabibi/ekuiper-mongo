package main

import (
	"testing"
)

func TestCollect_BufferFull_Drop(t *testing.T) {
	cfg := newDefaultConfig()
	cfg.MaxBufferSize = 2
	cfg.OnBufferFull = "drop"
	m := newSinkWithMock(cfg, &mockInserter{})
	m.buffer = []interface{}{"a", "b"}

	err := m.Collect(nil, "c")
	if err != nil {
		t.Errorf("drop mode should not return error, got: %v", err)
	}
	m.mu.Lock()
	n := len(m.buffer)
	m.mu.Unlock()
	if n != 2 {
		t.Errorf("buffer should still have 2 items, got %d", n)
	}
}

func TestCollect_BufferFull_Block(t *testing.T) {
	cfg := newDefaultConfig()
	cfg.MaxBufferSize = 2
	cfg.OnBufferFull = "block"
	m := newSinkWithMock(cfg, &mockInserter{})
	m.buffer = []interface{}{"a", "b"}

	err := m.Collect(nil, "c")
	if err == nil {
		t.Error("block mode should return error when buffer full")
	}
}

func TestCollect_AppendWhenBelowLimit(t *testing.T) {
	cfg := newDefaultConfig()
	cfg.MaxBufferSize = 10
	m := newSinkWithMock(cfg, &mockInserter{})
	_ = m.Collect(nil, []byte(`[{"x":1}]`))
	_ = m.Collect(nil, []byte(`[{"y":2}]`))
	m.mu.Lock()
	n := len(m.buffer)
	m.mu.Unlock()
	if n != 2 {
		t.Errorf("expected 2 buffered items, got %d", n)
	}
}
