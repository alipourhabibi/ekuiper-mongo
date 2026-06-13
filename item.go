package main

import (
	"encoding/json"
	"fmt"
)

// normaliseItem converts any Collect() item into a slice of MongoDB documents.
// Handles: []byte/[]uint8 (JSON array or object), map[string]interface{},
// []interface{}, []map[string]interface{}, and arbitrary marshallable types.
// Never panics; returns an error for unrepresentable types.
func normaliseItem(item interface{}) ([]interface{}, error) {
	switch v := item.(type) {
	case []byte: // []byte == []uint8
		return decodeJSON(v)
	case map[string]interface{}:
		return []interface{}{v}, nil
	case []interface{}:
		return v, nil
	case []map[string]interface{}:
		out := make([]interface{}, len(v))
		for i, m := range v {
			out[i] = m
		}
		return out, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("normaliseItem: cannot marshal %T: %w", item, err)
		}
		return decodeJSON(b)
	}
}

// decodeJSON decodes a JSON byte slice into a slice of documents.
// Tries array first; falls back to single object.
func decodeJSON(b []byte) ([]interface{}, error) {
	var arr []interface{}
	if err := json.Unmarshal(b, &arr); err == nil {
		return arr, nil
	}
	var obj interface{}
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, fmt.Errorf("decodeJSON: %w", err)
	}
	return []interface{}{obj}, nil
}
