package store

import (
	"github.com/arriqaaq/hash"
	"github.com/arriqaaq/zset"
	"taodb/global"
	"time"
)

func NewZSetStore() *ZSetStore {
	n := &ZSetStore{}
	n.ZSet = zset.New()
	return n
}

func (z *ZSetStore) Evict(cache *hash.Hash) {
	z.Lock()
	defer z.Unlock()

	keys := z.Keys()
	expiredKeys := make([]string, 0, 1)

	for _, k := range keys {
		ttl := cache.HGet(global.ZSet, k)
		if ttl == nil {
			continue
		}
		if time.Now().Unix() > ttl.(int64) {
			expiredKeys = append(expiredKeys, k)
		}
	}

	for _, k := range expiredKeys {
		z.ZClear(k)
		cache.HDel(global.ZSet, k)
	}
}
