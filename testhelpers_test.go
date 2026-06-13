package main

import (
	"context"
	"errors"
	"sync"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// noopLogger satisfies api.Logger without printing anything.
type noopLogger struct{}

func (noopLogger) Debug(args ...interface{})            {}
func (noopLogger) Info(args ...interface{})             {}
func (noopLogger) Warn(args ...interface{})             {}
func (noopLogger) Error(args ...interface{})            {}
func (noopLogger) Debugln(args ...interface{})          {}
func (noopLogger) Infoln(args ...interface{})           {}
func (noopLogger) Warnln(args ...interface{})           {}
func (noopLogger) Errorln(args ...interface{})          {}
func (noopLogger) Debugf(f string, args ...interface{}) {}
func (noopLogger) Infof(f string, args ...interface{})  {}
func (noopLogger) Warnf(f string, args ...interface{})  {}
func (noopLogger) Errorf(f string, args ...interface{}) {}

// mockInserter records InsertMany calls and can simulate failures.
type mockInserter struct {
	mu       sync.Mutex
	calls    int
	failFor  int // fail this many calls before succeeding
	lastDocs []interface{}
	failWith error
}

func (m *mockInserter) InsertMany(ctx context.Context, docs []interface{}, _ ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.failFor > 0 {
		m.failFor--
		err := m.failWith
		if err == nil {
			err = errors.New("transient error")
		}
		return nil, err
	}
	m.lastDocs = docs
	ids := make([]interface{}, len(docs))
	for i := range ids {
		ids[i] = primitive.NewObjectID()
	}
	return &mongo.InsertManyResult{InsertedIDs: ids}, nil
}

func (m *mockInserter) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// countingInserter counts total documents inserted across all calls.
type countingInserter struct {
	delegate *mockInserter
	total    *int64
}

func (c *countingInserter) InsertMany(ctx context.Context, docs []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	// Import sync/atomic inline via pointer arithmetic is not needed — just use a mutex-guarded int.
	c.delegate.mu.Lock()
	*c.total += int64(len(docs))
	c.delegate.mu.Unlock()
	return c.delegate.InsertMany(ctx, docs, opts...)
}

func newSinkWithMock(cfg mongoConfig, ins *mockInserter) *mongoSink {
	return &mongoSink{
		cfg:     cfg,
		colFunc: func() inserter { return ins },
		log:     noopLogger{},
	}
}
