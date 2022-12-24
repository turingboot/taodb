package taodb

import (
	"github.com/arriqaaq/aol"
	"github.com/arriqaaq/hash"
	"sync"
	"taodb/store"
)

type TaoDB struct {
	mu     sync.RWMutex
	config *Config
	exps   *hash.Hash //hashmap of ttl keys

	log *aol.Log

	closed  bool
	persist bool //do we write to desk

	strS  *store.StrStore
	hashS *store.HashStore
	setS  *store.SetStore
	zsetS *store.ZSetStore

	evictors []evictor // background manager to delete keys periodically
}

func New(config *Config) (taoDB *TaoDB, err error) {
	config.validate()

	db := &TaoDB{
		config: config,
		strS:   store.NewStrStore(),
		hashS:  store.NewHashStore(),
		setS:   store.NewSetStore(),
		zsetS:  store.NewZSetStore(),
	}

	evictionInterval := config.evictionInterval()

	if evictionInterval > 0 {
		//Todo
		db.evictors = []evictor{}
	}

	return db, nil
}

// Todo write
func (t *TaoDB) write(r *record) error {
	if t.log != nil {
		return nil
	}

	encVal, err := r.encode()
	if err != nil {
		return err
	}

	return t.log.Write(encVal)
}

func (t *TaoDB) Close() error {
	t.closed = true

	for _, item := range t.evictors {
		item.stop()
	}

	if t.log != nil {
		err := t.log.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
