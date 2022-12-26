package taodb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"taodb/global"
	"testing"
)

var (
	testKey = "dummy"
	tmpDir  = "tmp"
)

func testConfig() *Config {
	return &Config{
		Addr:   DefaultAddr,
		Path:   tmpDir,
		NoSync: true,
	}
}

func getTestDB() *TaoDB {
	db, _ := New(testConfig())
	return db
}

func TestTx_SetGet(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)
	if err := db.Update(func(tx *Tx) error {
		err := tx.Set("foo", "bar")
		assert.Equal(t, nil, err)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		val, err := tx.Get("foo")
		assert.Equal(t, nil, err)
		assert.Equal(t, "bar", val)
		fmt.Printf("key:%s,value:%s\n", "foo", val)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTx_SetEx(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		err := tx.SetEx("foo", "1", -4)
		assert.NotEmpty(t, err)

		err = tx.SetEx("foo", "1", 993)
		assert.Empty(t, err)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFlashDB_Delete(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		err := tx.Set("foo", "bar")
		assert.Equal(t, err, nil)

		err = tx.Delete("foo")
		assert.Equal(t, err, nil)

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		val, err := tx.Get("foo")
		assert.Empty(t, val)
		assert.Equal(t, global.ErrInvalidKey, err)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

}

func TestFlashDB_TTL(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		err := tx.SetEx("foo", "bar", 20)
		assert.Equal(t, err, nil)

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		ttl := tx.TTL("foo")
		assert.Equal(t, int(ttl), 20)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

}
