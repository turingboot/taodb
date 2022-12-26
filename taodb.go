package taodb

import (
	"github.com/arriqaaq/aol"
	"github.com/arriqaaq/hash"
	"sync"
	"taodb/global"
	"taodb/store"
	"time"
)

type TaoDB struct {
	Mu     sync.RWMutex
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
		exps:   hash.New(),
	}

	evictionInterval := config.evictionInterval()

	if evictionInterval > 0 {
		db.evictors = []evictor{
			newSweeperWithStore(db.strS, evictionInterval),
			newSweeperWithStore(db.hashS, evictionInterval),
			newSweeperWithStore(db.setS, evictionInterval),
			newSweeperWithStore(db.zsetS, evictionInterval),
		}

		for _, evictor := range db.evictors {
			go evictor.run(db.exps)
		}

	}

	db.persist = config.Path != ""
	if db.persist {
		opts := aol.DefaultOptions
		opts.NoSync = config.NoSync

		l, err := aol.Open(config.Path, opts)
		if err != nil {
			return nil, err
		}

		db.log = l

		//Todo 这里是有问题的，需要解决
		// load data from append-only log
		//err = db.load()
		//if err != nil {
		//	return nil, err
		//}
	}

	return db, nil
}

func (t *TaoDB) setTTL(dType global.DataType, key string, ttl int64) {
	t.exps.HSet(dType, key, ttl)
}

func (t *TaoDB) getTTL(dType global.DataType, key string) interface{} {
	return t.exps.HGet(dType, key)
}

func (t *TaoDB) hasExpired(key string, dType global.DataType) (expired bool) {
	ttl := t.exps.HGet(dType, key)
	if ttl == nil {
		return
	}
	if time.Now().Unix() > ttl.(int64) {
		expired = true
	}
	return
}

func (t *TaoDB) evict(key string, dType global.DataType) {
	ttl := t.exps.HGet(dType, key)
	if ttl == nil {
		return
	}

	var r *Record
	if time.Now().Unix() > ttl.(int64) {
		switch dType {
		case global.String:
			r = newRecord([]byte(key), nil, global.StringRecord, global.StringRem)
			t.strS.Delete([]byte(key))
		case global.Hash:
			r = newRecord([]byte(key), nil, global.HashRecord, global.HashHClear)
			t.hashS.HClear(key)
		case global.Set:
			r = newRecord([]byte(key), nil, global.SetRecord, global.SetSClear)
			t.setS.SClear(key)
		case global.ZSet:
			r = newRecord([]byte(key), nil, global.ZSetRecord, global.ZSetZClear)
			t.zsetS.ZClear(key)
		}

		if err := t.write(r); err != nil {
			panic(err)
		}

		t.exps.HDel(dType, key)
	}
}

func (t *TaoDB) write(r *Record) error {
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
