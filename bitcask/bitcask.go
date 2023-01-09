package bitcask

import "sync"

type BitCask struct {
	rwLock *sync.RWMutex
}
