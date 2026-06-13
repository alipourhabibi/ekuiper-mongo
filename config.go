package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultInterval            = 1000 * time.Millisecond
	defaultConnString          = "mongodb://localhost:27017/"
	defaultDBName              = "ekuiper"
	defaultColName             = "ekuiper"
	defaultMaxPoolSize  uint64 = 16
	defaultConnTimeout         = 5 * time.Second
	defaultSrvSelTimeout       = 30 * time.Second
	defaultMaxBufferSize       = 0
	defaultOnBufferFull        = "drop"
	defaultMaxRetries          = 3
	defaultRetryBaseDelay      = 200 * time.Millisecond
)

type mongoConfig struct {
	Interval               time.Duration
	ConnString             string
	DBName                 string
	ColName                string
	MaxPoolSize            uint64
	ConnectTimeout         time.Duration
	ServerSelectionTimeout time.Duration
	MaxBufferSize          int
	OnBufferFull           string // "drop" | "block"
	MaxRetries             int
	RetryBaseDelay         time.Duration
}

func newDefaultConfig() mongoConfig {
	return mongoConfig{
		Interval:               defaultInterval,
		ConnString:             defaultConnString,
		DBName:                 defaultDBName,
		ColName:                defaultColName,
		MaxPoolSize:            defaultMaxPoolSize,
		ConnectTimeout:         defaultConnTimeout,
		ServerSelectionTimeout: defaultSrvSelTimeout,
		MaxBufferSize:          defaultMaxBufferSize,
		OnBufferFull:           defaultOnBufferFull,
		MaxRetries:             defaultMaxRetries,
		RetryBaseDelay:         defaultRetryBaseDelay,
	}
}

// parseConfig extracts typed values from the eKuiper props map.
// Wrong-typed values append a warning and fall back to defaults.
// Invalid logical values return an error.
func parseConfig(props map[string]interface{}) (mongoConfig, []string, error) {
	cfg := newDefaultConfig()
	var warnings []string

	if raw, ok := props["interval"]; ok {
		switch v := raw.(type) {
		case float64:
			cfg.Interval = time.Duration(int64(v)) * time.Millisecond
		case int:
			cfg.Interval = time.Duration(v) * time.Millisecond
		default:
			warnings = append(warnings, fmt.Sprintf("interval: unexpected type %T, using default", raw))
		}
	}

	stringProp := func(key string, dst *string) {
		if raw, ok := props[key]; ok {
			if s, ok := raw.(string); ok {
				*dst = s
			} else {
				warnings = append(warnings, fmt.Sprintf("%s: unexpected type %T, using default", key, raw))
			}
		}
	}
	stringProp("conString", &cfg.ConnString)
	stringProp("dbName", &cfg.DBName)
	stringProp("colName", &cfg.ColName)
	stringProp("onBufferFull", &cfg.OnBufferFull)

	intProp := func(key string, dst *int) {
		if raw, ok := props[key]; ok {
			if v, ok := raw.(float64); ok {
				*dst = int(v)
			} else {
				warnings = append(warnings, fmt.Sprintf("%s: unexpected type %T, using default", key, raw))
			}
		}
	}
	intProp("maxBufferSize", &cfg.MaxBufferSize)
	intProp("maxRetries", &cfg.MaxRetries)

	if raw, ok := props["maxPoolSize"]; ok {
		if v, ok := raw.(float64); ok {
			cfg.MaxPoolSize = uint64(v)
		} else {
			warnings = append(warnings, fmt.Sprintf("maxPoolSize: unexpected type %T, using default", raw))
		}
	}

	durationMsProp := func(key string, dst *time.Duration) {
		if raw, ok := props[key]; ok {
			if v, ok := raw.(float64); ok {
				*dst = time.Duration(int64(v)) * time.Millisecond
			} else {
				warnings = append(warnings, fmt.Sprintf("%s: unexpected type %T, using default", key, raw))
			}
		}
	}
	durationMsProp("connectTimeoutMs", &cfg.ConnectTimeout)
	durationMsProp("serverSelTimeoutMs", &cfg.ServerSelectionTimeout)

	return cfg, warnings, cfg.validate()
}

func (c mongoConfig) validate() error {
	if c.Interval <= 0 {
		return errors.New("interval must be positive")
	}
	if c.ConnString == "" {
		return errors.New("conString is required")
	}
	if c.DBName == "" {
		return errors.New("dbName is required")
	}
	if c.ColName == "" {
		return errors.New("colName is required")
	}
	if c.OnBufferFull != "drop" && c.OnBufferFull != "block" {
		return fmt.Errorf("onBufferFull must be \"drop\" or \"block\", got %q", c.OnBufferFull)
	}
	if c.MaxRetries < 1 {
		return errors.New("maxRetries must be at least 1")
	}
	return nil
}
