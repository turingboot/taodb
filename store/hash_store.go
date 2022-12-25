package store

import (
	"github.com/arriqaaq/hash"
	"taodb/global"
	"time"
)

func NewHashStore() *HashStore {
	n := &HashStore{}
	n.Hash = hash.New()
	return n
}

func (h *HashStore) Evict(cache *hash.Hash) {
	h.Lock()
	defer h.Unlock()

	keys := h.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(global.Hash, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		h.HClear(k)
		cache.HDel(global.Hash, k)
	}
}
