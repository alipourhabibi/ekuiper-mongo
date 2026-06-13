package main

import (
	"testing"
	"time"
)

func TestConfigure_Defaults(t *testing.T) {
	cfg, warns, err := parseConfig(map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}
	if cfg.Interval != defaultInterval {
		t.Errorf("interval: got %v, want %v", cfg.Interval, defaultInterval)
	}
	if cfg.ConnString != defaultConnString {
		t.Errorf("connString: got %q", cfg.ConnString)
	}
	if cfg.DBName != defaultDBName {
		t.Errorf("dbName: got %q", cfg.DBName)
	}
	if cfg.ColName != defaultColName {
		t.Errorf("colName: got %q", cfg.ColName)
	}
	if cfg.MaxPoolSize != defaultMaxPoolSize {
		t.Errorf("maxPoolSize: got %d", cfg.MaxPoolSize)
	}
	if cfg.MaxBufferSize != defaultMaxBufferSize {
		t.Errorf("maxBufferSize: got %d", cfg.MaxBufferSize)
	}
	if cfg.OnBufferFull != defaultOnBufferFull {
		t.Errorf("onBufferFull: got %q", cfg.OnBufferFull)
	}
	if cfg.MaxRetries != defaultMaxRetries {
		t.Errorf("maxRetries: got %d", cfg.MaxRetries)
	}
}

func TestConfigure_ValidOverrides(t *testing.T) {
	props := map[string]interface{}{
		"interval":  float64(500),
		"conString": "mongodb://mongo:27017/",
		"dbName":    "mydb",
		"colName":   "mycol",
	}
	cfg, _, err := parseConfig(props)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Interval != 500*time.Millisecond {
		t.Errorf("interval: got %v", cfg.Interval)
	}
	if cfg.ConnString != "mongodb://mongo:27017/" {
		t.Errorf("connString: got %q", cfg.ConnString)
	}
	if cfg.DBName != "mydb" {
		t.Errorf("dbName: got %q", cfg.DBName)
	}
	if cfg.ColName != "mycol" {
		t.Errorf("colName: got %q", cfg.ColName)
	}
}

func TestConfigure_WrongTypeFallsToDefault(t *testing.T) {
	cfg, warns, err := parseConfig(map[string]interface{}{"interval": "not-a-number"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warns) == 0 {
		t.Error("expected warning for wrong type, got none")
	}
	if cfg.Interval != defaultInterval {
		t.Errorf("should fall back to default interval, got %v", cfg.Interval)
	}
}

func TestConfigure_InvalidInterval(t *testing.T) {
	_, _, err := parseConfig(map[string]interface{}{"interval": float64(0)})
	if err == nil {
		t.Error("expected error for interval=0")
	}
}

func TestConfigure_InvalidOnBufferFull(t *testing.T) {
	_, _, err := parseConfig(map[string]interface{}{"onBufferFull": "drain"})
	if err == nil {
		t.Error("expected error for invalid onBufferFull")
	}
}

func TestConfigure_EmptyConnString(t *testing.T) {
	_, _, err := parseConfig(map[string]interface{}{"conString": ""})
	if err == nil {
		t.Error("expected error for empty conString")
	}
}
