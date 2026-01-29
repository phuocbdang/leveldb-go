package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	dbDir := "db"
	os.RemoveAll(dbDir)

	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d", i)

		if err := db.Put([]byte(key), []byte(value)); err != nil {
			log.Fatal(err)
		}
	}

	db.Close()

	db2, err := NewDB(dbDir)
	if err != nil {
		log.Fatal("failed to reopen db")
	}

	keyToFind := []byte("key_010")
	val, ok := db2.Get(keyToFind)
	if ok {
		log.Println(string(val))
	} else {
		log.Fatalf("key not found")
	}

	keyToFind = []byte("ke_010")
	val, ok = db2.Get(keyToFind)
	if ok {
		log.Println(string(val))
	} else {
		log.Fatalf("key not found")
	}
}
