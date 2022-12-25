package taodb

import (
	"github.com/arriqaaq/aol"
	"taodb/global"
	"time"
)

// load String, Hash, Set and ZSet stores from append-only log
func (t *TaoDB) load() error {
	if t.log == nil {
		return nil
	}

	noOfSegments := t.log.Segments()
	for i := 1; i <= noOfSegments; i++ {
		j := 0

		for {
			data, err := t.log.Read(uint64(i), uint64(j))
			if err != nil {
				if err == aol.ErrEOF {
					break
				}
				return err
			}

			record, err := decode(data)
			if err != nil {
				return err
			}

			if len(record.meta.key) > 0 {
				if err := t.loadRecord(record); err != nil {
					return err
				}
			}

			j++
		}
	}

	return nil
}

func (t *TaoDB) loadRecord(r *Record) (err error) {

	switch r.getType() {
	case global.StringRecord:
		err = t.buildStringRecord(r)
	case global.HashRecord:
		err = t.buildHashRecord(r)
	case global.SetRecord:
		err = t.buildSetRecord(r)
	case global.ZSetRecord:
		err = t.buildZsetRecord(r)
	}
	return
}

/*
	Utility functions to build stores from aol Record
*/

func (t *TaoDB) buildStringRecord(r *Record) error {

	key := string(r.meta.key)
	member := string(r.meta.member)

	switch r.getMark() {
	case global.StringSet:
		t.strS.Insert([]byte(key), member)
	case global.StringRem:
		t.strS.Delete([]byte(key))
		t.exps.HDel(global.String, key)
	case global.StringExpire:
		if r.timestamp < uint64(time.Now().Unix()) {
			t.strS.Delete([]byte(key))
			t.exps.HDel(global.String, key)
		} else {
			t.setTTL(global.String, key, int64(r.timestamp))
		}
	}

	return nil
}

func (t *TaoDB) buildHashRecord(r *Record) error {

	key := string(r.meta.key)
	member := string(r.meta.member)
	value := string(r.meta.value)

	switch r.getMark() {
	case global.HashHSet:
		t.hashS.HSet(key, member, value)
	case global.HashHDel:
		t.hashS.HDel(key, member)
	case global.HashHClear:
		t.hashS.HClear(key)
		t.exps.HDel(global.Hash, key)
	case global.HashHExpire:
		if r.timestamp < uint64(time.Now().Unix()) {
			t.hashS.HClear(key)
			t.exps.HDel(global.Hash, key)
		} else {
			t.setTTL(global.Hash, key, int64(r.timestamp))
		}
	}

	return nil
}

func (t *TaoDB) buildSetRecord(r *Record) error {

	key := string(r.meta.key)
	member := string(r.meta.member)
	value := string(r.meta.value)

	switch r.getMark() {
	case global.SetSAdd:
		t.setS.SAdd(key, member)
	case global.SetSRem:
		t.setS.SRem(key, member)
	case global.SetSMove:
		t.setS.SMove(key, value, member)
	case global.SetSClear:
		t.setS.SClear(key)
		t.exps.HDel(global.Set, key)
	case global.SetSExpire:
		if r.timestamp < uint64(time.Now().Unix()) {
			t.setS.SClear(key)
			t.exps.HDel(global.Set, key)
		} else {
			t.setTTL(global.Set, key, int64(r.timestamp))
		}
	}

	return nil
}

func (t *TaoDB) buildZsetRecord(r *Record) error {

	key := string(r.meta.key)
	member := string(r.meta.member)
	value := string(r.meta.value)

	switch r.getMark() {
	case global.ZSetZAdd:
		score, err := strToFloat64(value)
		if err != nil {
			return err
		}
		t.zsetS.ZAdd(key, score, member, nil)
	case global.ZSetZRem:
		t.zsetS.ZRem(key, member)
	case global.ZSetZClear:
		t.zsetS.ZClear(key)
		t.exps.HDel(global.ZSet, key)
	case global.ZSetZExpire:
		if r.timestamp < uint64(time.Now().Unix()) {
			t.zsetS.ZClear(key)
			t.exps.HDel(global.ZSet, key)
		} else {
			t.setTTL(global.ZSet, key, int64(r.timestamp))
		}
	}

	return nil
}
