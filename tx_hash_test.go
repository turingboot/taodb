package taodb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestTaoDB_HGetSet(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		_, err := tx.HSet(testKey, "bar", "1")
		assert.NoError(t, err)
		_, err = tx.HSet(testKey, "baz", "2")
		assert.NoError(t, err)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		val := tx.HGet(testKey, "bar")
		assert.Equal(t, "1", val)
		val = tx.HGet(testKey, "baz")
		assert.Equal(t, "2", val)
		return nil
	})
}

func TestTaoDB_HGetAll(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		tx.HSet(testKey, "bar", "1")
		tx.HSet(testKey, "baz", "2")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		values := tx.HGetAll(testKey)
		assert.Equal(t, 4, len(values))
		return nil
	})
}

func TestTaoDB_HDel(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		tx.HSet(testKey, "bar", "1")
		tx.HSet(testKey, "baz", "2")
		res, err := tx.HDel(testKey, "bar", "baz")
		assert.Nil(t, err)
		assert.Equal(t, 2, res)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		assert.Empty(t, tx.HGet(testKey, "bar"))
		assert.Empty(t, tx.HGet(testKey, "baz"))
		return nil
	})
}

func TestTaoDB_HExists(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		tx.HSet(testKey, "bar", "1")
		tx.HSet(testKey, "baz", "2")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		assert.True(t, tx.HExists(testKey, "bar"))
		assert.True(t, tx.HExists(testKey, "baz"))
		assert.False(t, tx.HExists(testKey, "ben"))
		return nil
	})
}

func TestTaoDB_HKeyExists(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		tx.HSet(testKey, "bar", "1")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		assert.True(t, tx.HKeyExists(testKey))
		assert.False(t, tx.HKeyExists("yolo"))
		return nil
	})
}

func TestTaoDB_HLen(t *testing.T) {
	db := getTestDB()
	defer db.Close()
	defer os.RemoveAll(tmpDir)

	if err := db.Update(func(tx *Tx) error {
		tx.HSet(testKey, "bar", "1")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *Tx) error {
		assert.Equal(t, tx.HLen(testKey), 1)
		return nil
	})
}
