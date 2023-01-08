package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
)

var (
	entryHeaderSize uint32 = 16
	hintHeaderSize  uint32 = 24
)

// the entry stored format:
// size = 4+4+4+4=16
//
//	|----------------------------------------------------------------------------------------------------------------|
//	|  crc  | timestamp | ksz | value_sz |   key  | value  |
//	|----------------------------------------------------------------------------------------------------------------|
//	| uint32| uint32  |uint32 |  uint32  | []byte | []byte |
//	|----------------------------------------------------------------------------------------------------------------|
type entry struct {
	crc   uint32
	key   []byte
	value []byte
	meta  *metaData
	mu    *sync.RWMutex
}

// the hint stored format:
// size = 4+4+8+4 = 20
//
//	|----------------------------------------------------------------------------------------------------------------|
//	|  timeStamp | key_sz | value_sz | value_ops |   key  |
//	|----------------------------------------------------------------------------------------------------------------|
//	|   uint32 | uint32   |  uint32   |  uint64   |  []byte |
//	|----------------------------------------------------------------------------------------------------------------|
type hint struct {
	key  []byte
	meta *metaData
	mu   *sync.RWMutex
}

type metaData struct {
	timeStamp   uint32
	fileID      uint32
	keySize     uint32
	valueSize   uint32
	valueOffset uint64
}

func newMetaData(timeStamp uint32, fileID uint32, keySize uint32, valueSize uint32, valueOffset uint64) *metaData {
	return &metaData{timeStamp: timeStamp, fileID: fileID, keySize: keySize, valueSize: valueSize, valueOffset: valueOffset}
}

func (et *entry) setEntryHeaderBuff(buff []byte) []byte {
	binary.LittleEndian.PutUint32(buff[4:8], et.meta.timeStamp)
	binary.LittleEndian.PutUint32(buff[8:12], et.meta.keySize)
	binary.LittleEndian.PutUint32(buff[12:16], et.meta.valueSize)

	return buff
}

func (et *entry) Encode() []byte {
	keySize := et.meta.keySize
	valueSize := et.meta.valueSize

	buff := make([]byte, et.Len())
	buff = et.setEntryHeaderBuff(buff)

	copy(buff[entryHeaderSize:entryHeaderSize+keySize], et.key)
	copy(buff[entryHeaderSize+keySize:entryHeaderSize+keySize+valueSize], et.value)

	crcValue := crc32.ChecksumIEEE(buff[4:])
	binary.LittleEndian.PutUint32(buff[0:4], crcValue)
	return buff
}

func (et *entry) Decode(buff []byte) (*entry, error) {

	crc := binary.BigEndian.Uint32(buff[0:4])
	if crc32.ChecksumIEEE(buff[4:]) != crc {
		return nil, errors.New("ChecksumIEEE error")
	}

	timestamp := binary.BigEndian.Uint32(buff[4:8])
	ks := binary.BigEndian.Uint32(buff[8:12])
	vs := binary.BigEndian.Uint32(buff[12:14])

	key, value := make([]byte, ks), make([]byte, vs)
	copy(key, buff[entryHeaderSize:entryHeaderSize+ks])
	copy(value, buff[entryHeaderSize+ks:entryHeaderSize+ks+vs])

	tmpEntry := &entry{
		crc: crc,
		meta: &metaData{
			timeStamp: timestamp,
			keySize:   ks,
			valueSize: vs,
		},
		key:   key,
		value: value,
	}
	return tmpEntry, nil
}

func (et *entry) Len() int64 {
	return int64(entryHeaderSize + et.meta.keySize + et.meta.valueSize)
}

func (h *hint) setHintHeaderBuff(buff []byte) []byte {
	binary.LittleEndian.PutUint32(buff[0:4], h.meta.timeStamp)
	binary.LittleEndian.PutUint32(buff[4:8], h.meta.keySize)
	binary.LittleEndian.PutUint32(buff[8:12], h.meta.valueSize)
	binary.LittleEndian.PutUint64(buff[12:hintHeaderSize], h.meta.valueOffset)

	return buff
}

func (h *hint) Len() int64 {
	return int64(entryHeaderSize + h.meta.keySize)
}

func (h *hint) Encode() []byte {
	keySize := h.meta.keySize
	buff := make([]byte, h.Len())
	buff = h.setHintHeaderBuff(buff)
	copy(buff[hintHeaderSize:hintHeaderSize+keySize], h.key)
	return buff
}

func (h *hint) Decode(buff []byte) *hint {

	timestamp := binary.LittleEndian.Uint32(buff[:4])
	ks := binary.LittleEndian.Uint32(buff[4:8])
	vs := binary.LittleEndian.Uint32(buff[8:12])
	vf := binary.LittleEndian.Uint64(buff[12:hintHeaderSize])

	key := make([]byte, ks)
	copy(key, buff[hintHeaderSize:hintHeaderSize+ks])
	tmpHint := &hint{
		meta: &metaData{
			timeStamp:   timestamp,
			keySize:     ks,
			valueSize:   vs,
			valueOffset: vf,
		},
		key: key,
	}
	return tmpHint
}

type Record struct {
	e *entry
	h *hint
}
