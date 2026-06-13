package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// inserter is the subset of mongo.Collection used by flush.
// Defined as an interface so tests can inject a mock without a real MongoDB.
type inserter interface {
	InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
}

type mongoSink struct {
	mu      sync.Mutex
	cfg     mongoConfig
	client  *mongo.Client
	colFunc func() inserter // returns target collection; injectable for tests
	buffer  []interface{}
	log     api.Logger
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Configure is called once before Open. Returns an error for invalid config
// so eKuiper rejects the rule immediately rather than failing at runtime.
func (m *mongoSink) Configure(props map[string]interface{}) error {
	cfg, _, err := parseConfig(props)
	if err != nil {
		return fmt.Errorf("mongoSink configure: %w", err)
	}
	m.cfg = cfg
	return nil
}

// Open establishes the MongoDB connection and starts the periodic flush goroutine.
func (m *mongoSink) Open(ctx api.StreamContext) error {
	m.log = ctx.GetLogger()

	clientOpts := options.Client().
		ApplyURI(m.cfg.ConnString).
		SetRetryWrites(true).
		SetMaxPoolSize(m.cfg.MaxPoolSize).
		SetConnectTimeout(m.cfg.ConnectTimeout).
		SetServerSelectionTimeout(m.cfg.ServerSelectionTimeout)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("mongoSink open: connect: %w", err)
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return fmt.Errorf("mongoSink open: ping: %w", err)
	}
	m.client = client
	m.colFunc = func() inserter {
		return client.Database(m.cfg.DBName).Collection(m.cfg.ColName)
	}

	ticker := time.NewTicker(m.cfg.Interval)
	execCtx, cancel := ctx.WithCancel()
	m.cancel = cancel

	m.wg.Add(1)
	go m.flushLoop(execCtx, ticker)

	m.log.Infof("mongoSink: opened - flushing every %v to %s.%s",
		m.cfg.Interval, m.cfg.DBName, m.cfg.ColName)
	return nil
}

// Collect buffers incoming stream items. Applies backpressure when MaxBufferSize is set.
func (m *mongoSink) Collect(_ api.StreamContext, item interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cfg.MaxBufferSize > 0 && len(m.buffer) >= m.cfg.MaxBufferSize {
		switch m.cfg.OnBufferFull {
		case "drop":
			m.log.Warnf("mongoSink: buffer full (%d), dropping item", m.cfg.MaxBufferSize)
			return nil
		case "block":
			return fmt.Errorf("mongoSink: buffer full (%d)", m.cfg.MaxBufferSize)
		}
	}

	m.buffer = append(m.buffer, item)
	return nil
}

// Close signals the flush goroutine, waits for the final flush, then disconnects.
func (m *mongoSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	if m.client != nil {
		if err := m.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("mongoSink close: disconnect: %w", err)
		}
	}
	m.log.Infof("mongoSink: closed")
	return nil
}
