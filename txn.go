package taodb

type Tx struct {
	db       *TaoDB
	writable bool
	wc       *txWriteContext
}

type txWriteContext struct {
	commitItems []*record // details for committing tx.
}
