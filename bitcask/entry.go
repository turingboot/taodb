package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
)

var (
	entryHeaderSize uint32 = 16
	hintHeaderSize  uint32 = 20
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
//	|  file_id | value_sz | value_pos | timeStamp |   key  |
//	|----------------------------------------------------------------------------------------------------------------|
//	|   uint32 | uint32   |  uint64   |  uint32   |  []byte |
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

	return nil
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

func (et *entry) Decode(buff []byte) *entry {

	crc := binary.BigEndian.Uint32(buff[0:4])
	timestamp := binary.BigEndian.Uint32(buff[4:8])
	ks := binary.BigEndian.Uint32(buff[8:12])
	vs := binary.BigEndian.Uint32(buff[12:14])

	key, value := make([]byte, ks), make([]byte, vs)
	copy(key, buff[entryHeaderSize:entryHeaderSize+ks])
	copy(value, buff[entryHeaderSize+ks:entryHeaderSize+ks+vs])
	return &entry{
		crc: crc,
		meta: &metaData{
			timeStamp: timestamp,
			keySize:   ks,
			valueSize: vs,
		},
		key:   key,
		value: value,
	}

}

func (et *entry) Len() int64 {
	return int64(entryHeaderSize + et.meta.keySize + et.meta.valueSize)
}
