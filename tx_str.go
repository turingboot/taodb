package taodb

import (
	"taodb/global"
	"time"
)

// Set saves a key-value pair.
func (tx *Tx) Set(key string, value string) error {
	e := newRecord([]byte(key), []byte(value), global.StringRecord, global.StringSet)
	tx.addRecord(e)
	return nil
}

func (tx *Tx) SetEx(key string, value string, duration int64) (err error) {
	if duration < 0 {
		return ErrInvalidEntry
	}

	ttl := time.Now().Unix() + duration

	e := newRecordWithExpire([]byte(key), []byte(value), ttl,
		global.StringRecord, global.StringExpire)
	tx.addRecord(e)
	return
}

// Get returns value of the given key. It may return error if something goes wrong.
func (tx *Tx) Get(key string) (val string, err error) {
	val, err = tx.get(key)
	if err != nil {
		return
	}

	return
}

// Delete deletes the given key.
func (tx *Tx) Delete(key string) error {
	e := newRecord([]byte(key), nil, global.StringRecord, global.StringRem)
	tx.addRecord(e)

	return nil
}

// Expire adds an expiration time period to the given key.
func (tx *Tx) Expire(key string, duration int64) (err error) {
	if duration <= 0 {
		return global.ErrInvalidTTL
	}

	if _, err = tx.get(key); err != nil {
		return
	}

	ttl := time.Now().Unix() + duration
	e := newRecordWithExpire([]byte(key), nil, ttl, global.StringRecord, global.StringExpire)
	tx.addRecord(e)

	return
}

// TTL returns remaining time of the expiration.
func (tx *Tx) TTL(key string) (ttl int64) {
	deadline := tx.db.getTTL(global.String, key)
	if deadline == nil {
		return
	}

	if tx.db.hasExpired(key, global.String) {
		tx.db.evict(key, global.String)
		return
	}

	return deadline.(int64) - time.Now().Unix()
}

// Exists checks the given key whether exists. Also, if the key is expired,
// the key is evicted and return false.
func (tx *Tx) Exists(key string) bool {
	_, err := tx.db.strS.Get(key)
	if err != nil {
		if err == global.ErrExpiredKey {
			tx.db.evict(key, global.String)
		}
		return false
	}

	return true
}

// get is a helper method for retrieving value of the given key from the database.
func (tx *Tx) get(key string) (val string, err error) {
	v, err := tx.db.strS.Get(key)
	if err != nil {
		return "", err
	}

	// Check if the key is expired.
	if tx.db.hasExpired(key, global.String) {
		tx.db.evict(key, global.String)
		return "", global.ErrExpiredKey
	}

	val = v.(string)
	return
}
