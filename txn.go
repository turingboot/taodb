package taodb

import (
	"github.com/arriqaaq/aol"
	"taodb/global"
)

type Tx struct {
	db       *TaoDB
	writable bool
	wc       *txWriteContext
}

type txWriteContext struct {
	commitItems []*Record // details for committing tx.
}

func (tx *Tx) addRecord(r *Record) {
	tx.wc.commitItems = append(tx.wc.commitItems, r)
}

// 根据事务类型锁住数据库
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.Mu.Lock()
	} else {
		tx.db.Mu.RLock()
	}
}

// 根据事务类型解锁住数据库
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.Mu.Unlock()
	} else {
		tx.db.Mu.RUnlock()
	}
}

func (t *TaoDB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = t.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must roll back.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	err = fn(tx)
	return
}

func (t *TaoDB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       t,
		writable: writable,
	}
	tx.lock()
	if t.closed {
		tx.unlock()
		return nil, global.ErrDatabaseClosed
	}
	if writable {
		tx.wc = &txWriteContext{}
		if t.persist {
			tx.wc.commitItems = make([]*Record, 0, 1)
		}
	}
	return tx, nil
}

func (tx *Tx) Commit() error {
	if tx.db == nil {
		return global.ErrTxClosed
	} else if !tx.writable {
		return global.ErrTxNotWritable
	}

	var err error
	if tx.db.persist && (len(tx.wc.commitItems) > 0) && tx.writable {
		batch := new(aol.Batch)
		// 每条提交的记录都被写到磁盘上
		for _, r := range tx.wc.commitItems {
			rec, err := r.encode()
			if err != nil {
				return err
			}
			batch.Write(rec)
		}
		// 如果这个操作失败了，那么写入就失败了，我们必须回滚
		err = tx.db.log.WriteBatch(batch)
		if err != nil {
			tx.rollback()
		}
	}
	//应用所有命令
	err = tx.buildRecords(tx.wc.commitItems)
	tx.unlock()
	tx.db = nil
	return err
}

func (t *TaoDB) View(fn func(tx *Tx) error) error {
	return t.managed(false, fn)
}

func (t *TaoDB) Update(fn func(tx *Tx) error) error {
	return t.managed(true, fn)
}

func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return global.ErrTxClosed
	}

	if tx.writable {
		tx.rollback()
	}

	tx.unlock()
	tx.db = nil
	return nil
}

// rollback处理底层的回滚逻辑
// 旨在从Commit()和Rollback()调用
func (tx *Tx) rollback() {
	tx.wc.commitItems = nil
}

func (tx *Tx) buildRecords(recs []*Record) (err error) {
	for _, r := range recs {
		switch r.getType() {
		case global.StringRecord:
			err = tx.db.buildStringRecord(r)
		case global.HashRecord:
			err = tx.db.buildHashRecord(r)
		case global.SetRecord:
			err = tx.db.buildSetRecord(r)
		case global.ZSetRecord:
			err = tx.db.buildZsetRecord(r)
		}
	}
	return
}
