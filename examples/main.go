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

		for i := 0; i < 100; i++ {
			err := tx.Set(fmt.Sprintf("mykey%d", i), fmt.Sprintf("myvalue%d", i))
			if err != nil {
				return err
			}
		}
		return nil
	})
	err = db.View(func(tx *taodb.Tx) error {

		for i := 0; i < 100; i++ {
			val, err := tx.Get(fmt.Sprintf("mykey%d", i))
			if err != nil {
				return err
			}
			fmt.Printf("value is %s\n", val)
		}
		return nil
	})

}
