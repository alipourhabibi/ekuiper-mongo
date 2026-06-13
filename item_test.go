package main

import (
	"testing"
)

func TestNormaliseItem_BytesArray(t *testing.T) {
	docs, err := normaliseItem([]byte(`[{"a":1},{"b":2}]`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(docs))
	}
}

func TestNormaliseItem_BytesSingleObject(t *testing.T) {
	docs, err := normaliseItem([]byte(`{"x":42}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("expected 1 doc, got %d", len(docs))
	}
}

func TestNormaliseItem_Map(t *testing.T) {
	docs, err := normaliseItem(map[string]interface{}{"key": "val"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("expected 1 doc, got %d", len(docs))
	}
}

func TestNormaliseItem_SliceOfMaps(t *testing.T) {
	docs, err := normaliseItem([]map[string]interface{}{{"a": 1}, {"b": 2}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(docs))
	}
}

func TestNormaliseItem_NoPanic_UnknownType(t *testing.T) {
	_, err := normaliseItem(make(chan int))
	if err == nil {
		t.Error("expected error for unmarshalable type")
	}
}

func TestNormaliseItem_InvalidJSON(t *testing.T) {
	_, err := normaliseItem([]byte(`{bad json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
