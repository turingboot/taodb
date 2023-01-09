package bitcask

import (
	"errors"
	"os"
)

type Record struct {
	e *entry
	h *hint
}

type RWManager struct {
	fp         *os.File
	writeItems []*Record
}

func (r *RWManager) addRecord(record *Record) {
	r.writeItems = append(r.writeItems, record)
}

func (r *RWManager) writeRecords() error {
	if len(r.writeItems) <= 0 {
		return errors.New("records is null")
	}

	for _, item := range r.writeItems {
		//write data file
		_, err1 := appendWriteFile(r.fp, item.h.Encode())
		if err1 != nil {
			return err1
		}
		//write hint file
		_, err2 := appendWriteFile(r.fp, item.e.Encode())
		if err2 != nil {
			return err2
		}
	}

	return nil
}

// helper
// write/read data/hint files
func appendWriteFile(fp *os.File, buf []byte) (int, error) {
	stat, err := fp.Stat()
	if err != nil {
		return -1, err
	}

	return fp.WriteAt(buf, stat.Size())
}
