package store

import (
	"github.com/arriqaaq/art"
	"github.com/arriqaaq/hash"
	"taodb/global"
	"time"
)

func NewStrStore() *StrStore {
	n := &StrStore{}
	n.Tree = art.NewTree()
	//n.SkipList = skiplist.SlCreate()
	return n
}

func (s *StrStore) Get(key string) (val interface{}, err error) {
	val = s.Search([]byte(key))
	if val == nil {
		return nil, global.ErrInvalidKey
	}
	return
}

func (s *StrStore) Evict(cache *hash.Hash) {
	s.Lock()
	defer s.Unlock()

	keys := s.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(global.String, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		s.Delete([]byte(k))
		cache.HDel(global.String, k)
	}
}

func (s *StrStore) Keys() (keys []string) {
	s.Each(func(node *art.Node) {
		if node.IsLeaf() {
			key := string(node.Key())
			keys = append(keys, key)
		}
	})
	return
}
