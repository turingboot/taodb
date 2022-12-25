package tx

import "taodb"

type Tx struct {
	db       *taodb.TaoDB
	writable bool
	wc       *txWriteContext
}

type txWriteContext struct {
	commitItems []*taodb.Record // details for committing tx.
}
