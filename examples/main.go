package main

import (
	"fmt"
	"log"
	"taodb"
)

func main() {
	config := &taodb.Config{Path: "/tmp", EvictionInterval: 10}
	db, err := taodb.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *taodb.Tx) error {
		err := tx.Set("mykey", "myvalue")
		return err
	})

	err = db.View(func(tx *taodb.Tx) error {
		val, err := tx.Get("mykey")
		if err != nil {
			return err
		}
		fmt.Printf("value is %s\n", val)
		return nil
	})
}
