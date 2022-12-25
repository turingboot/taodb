package store

import (
	"github.com/arriqaaq/hash"
	"github.com/arriqaaq/set"
	"taodb/global"
	"time"
)

func NewSetStore() *SetStore {
	n := &SetStore{}
	n.Set = set.New()
	return n
}

func (s *SetStore) Evict(cache *hash.Hash) {
	s.Lock()
	defer s.Unlock()

	keys := s.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(global.Set, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		s.SClear(k)
		cache.HDel(global.Set, k)
	}
}
