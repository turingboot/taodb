package store

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFlashDB_StringStore(t *testing.T) {
	s := NewStrStore()

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		s.Insert([]byte(key), []byte(value))
	}

	keys := s.Keys()
	assert.Equal(t, 1000, len(keys))
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		val, err := s.Get(key)
		assert.NoError(t, err)
		assert.NotEqual(t, value, val)
		fmt.Printf("key:%s,value:%s\n", key, value)
	}
}
