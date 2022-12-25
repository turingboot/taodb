package taodb

import (
	"github.com/arriqaaq/hash"
	"math/rand"
	"runtime"
	"taodb/store"
	"time"
)

const (
	MinimumStartupTime = 500 * time.Millisecond
	MaximumStartupTime = 2 * MinimumStartupTime
)

// 用于在每个分片的启动前放置一个随机的延迟，以避免让各个分片同时锁定
func startupDelay() time.Duration {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	d, delta := MinimumStartupTime, MaximumStartupTime-MinimumStartupTime
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return d
}

type evictor interface {
	run(cache *hash.Hash)
	stop()
}

// 清扫器
type sweeper struct {
	store    store.Store
	interval time.Duration
	stopC    chan bool
}

func newSweeperWithStore(s store.Store, sweepTime time.Duration) evictor {
	var swp = &sweeper{
		interval: sweepTime,
		stopC:    make(chan bool),
		store:    s,
	}
	runtime.SetFinalizer(swp, stopSweeper)
	return swp
}

func stopSweeper(evictor evictor) {
	evictor.stop()
}

func (s *sweeper) run(cache *hash.Hash) {
	<-time.After(startupDelay())
	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-ticker.C:
			s.store.Evict(cache)
		case <-s.stopC:
			ticker.Stop()
			return
		}
	}
}

func (s *sweeper) stop() {
	s.stopC <- true
}
