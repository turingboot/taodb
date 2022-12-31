package main

import (
	"fmt"
	"taodb/ds/skiplist"
)

func main() {
	// Set (accepts any value)
	val := "test_val"

	n := skiplist.SlCreate()
	n.SlInsert("ec", val)
	n.SlInsert("dc", 123)
	n.SlInsert("ac", val)

	// Get
	node, _ := n.SlGet("ec")
	fmt.Println("value: ", node.Value())

	// Delete
	n.SlDelete("dc")
}
