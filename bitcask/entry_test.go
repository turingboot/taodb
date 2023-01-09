package bitcask

import (
	"bytes"
	"testing"
	"time"
)

func foo1() *entry {

	key := []byte("foo1")
	value := []byte("value1")
	return &entry{
		key:   key,
		value: value,
		meta: &metaData{
			timeStamp: uint32(time.Now().Unix()),
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		},
	}
}
func foo2() *entry {

	key := []byte("foo2")
	value := []byte("value2")
	return &entry{
		key:   key,
		value: value,
		meta: &metaData{
			timeStamp: uint32(time.Now().Unix()),
			keySize:   uint32(len(key)),
			valueSize: uint32(len(value)),
		},
	}
}

func TestEntry_Encode(t *testing.T) {
	// EncodeEntry
	buff := foo1().Encode()
	t.Log(buff)

}

func TestEntry_Decode(t *testing.T) {
	buff := foo1().Encode()
	ent, err := foo1().Decode(buff)
	if err != nil {
		t.Error(err)
	}
	t.Log(bytes.Compare(buff, ent.Encode()))
}

func Test_hint_Encode(t *testing.T) {

}

func Test_hint_Decode(t *testing.T) {

}
