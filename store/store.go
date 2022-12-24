package store

import (
	"github.com/arriqaaq/art"
	"github.com/arriqaaq/hash"
	"github.com/arriqaaq/set"
	"github.com/arriqaaq/zset"
	"sync"
)

type Store interface {
	evict(cache *hash.Hash)
}

type StrStore struct {
	sync.RWMutex
	*art.Tree
}

type HashStore struct {
	sync.RWMutex
	*hash.Hash
}

type SetStore struct {
	sync.RWMutex
	*set.Set
}

type ZSetStore struct {
	sync.RWMutex
	*zset.ZSet
}
