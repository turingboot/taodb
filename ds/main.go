package main

import (
	"fmt"
	"taodb/ds/skiplist"
)

func main() {
	// Set (accepts any value)
	val := "test_val"

	n := skiplist.Create()
	n.Insert("ec", val)
	n.Insert("dc", 123)
	n.Insert("ac", val)

	// Get
	node, _ := n.Get("ec")
	fmt.Println("value: ", node.Value())

	// Delete
	n.Delete("dc")
}
