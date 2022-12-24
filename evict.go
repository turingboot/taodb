package taodb

import "github.com/arriqaaq/hash"

type evictor interface {
	run(cache *hash.Hash)
	stop()
}
