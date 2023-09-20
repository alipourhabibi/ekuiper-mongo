package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"log"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	sdk.Start(os.Args, &sdk.PluginConfig{
		Name: "mongo",
		Sinks: map[string]sdk.NewSinkFunc{
			"mongoGo": func() api.Sink {
				return &mongoSink{}
			},
		},
	})
}

type mongoSink struct {
	sync.Mutex
	interval  float64
	conString string
	db        *mongo.Client
	cancel    context.CancelFunc
	results   []interface{}
	dbName    string
	colName   string
}

func (m *mongoSink) Configure(props map[string]interface{}) error {
	m.interval = 1000
	m.conString = "mongodb://localhost:27017/"
	m.dbName = "ekuiper"
	m.colName = "ekuiper"
	if i, ok := props["interval"]; ok {
		if i, ok := i.(float64); ok {
			m.interval = i
		}
	}
	if i, ok := props["conString"]; ok {
		if i, ok := i.(string); ok {
			m.conString = i
		}
	}
	if i, ok := props["dbName"]; ok {
		if i, ok := i.(string); ok {
			m.dbName = i
		}
	}
	if i, ok := props["colName"]; ok {
		if i, ok := i.(string); ok {
			m.colName = i
		}
	}
	return nil
}

func (m *mongoSink) Open(ctx api.StreamContext) error {
	db, err := mongo.Connect(ctx, options.Client().ApplyURI(m.conString), options.Client().SetRetryWrites(true))
	if err != nil {
		return err
	}
	err = db.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return err
	}
	m.db = db

	t := time.NewTicker(time.Duration(m.interval) * time.Millisecond)
	execCtx, cancel := ctx.WithCancel()
	m.cancel = cancel
	go func() {
		for {
			select {
			case <-t.C:
				m.save()
			case <-execCtx.Done():
				return
			}
		}
	}()
	return nil
}

func (m *mongoSink) save() {
	if len(m.results) == 0 {
		return
	}
	var ds []interface{}
	for _, v := range m.results {
		// Every v is array of 1 element
		var d []interface{}
		err := json.Unmarshal(v.([]uint8), &d)
		if err != nil {
			log.Println(err)
		}
		if len(d) > 0 {
			ds = append(ds, d...)
		}
	}
	_, err := m.db.Database(m.dbName).Collection(m.colName).InsertMany(context.Background(), ds)
	if err != nil {
		log.Println(err)
	}
	m.results = make([]interface{}, 0)
}

func (m *mongoSink) Collect(ctx api.StreamContext, item interface{}) error {
	m.Lock()
	m.results = append(m.results, item)
	m.Unlock()
	return nil
}

func (m *mongoSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	err := m.db.Disconnect(ctx)
	if err != nil {
		return err
	}
	return nil
}
