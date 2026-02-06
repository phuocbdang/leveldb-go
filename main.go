package main

import (
	"log"
	"os"
)

func main() {
	dbDir := "db"
	os.RemoveAll(dbDir)

	db, err := NewDB(dbDir)
	if err != nil {
		log.Fatalf("failed to create DB: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("apple"), []byte("red")); err != nil {
		log.Fatalf("put failed: %v", err)
	}
	if err := db.Put([]byte("banana"), []byte("yellow")); err != nil {
		log.Fatalf("put failed: %v", err)
	}
	if err := db.Put([]byte("cherry"), []byte("red")); err != nil {
		log.Fatalf("put failed: %v", err)
	}
	if err := db.Put([]byte("apple"), []byte("green")); err != nil {
		log.Fatalf("put failed: %v", err)
	}
	if err := db.Delete([]byte("banana")); err != nil {
		log.Fatalf("delete failed: %v", err)
	}

	iter := db.NewIterator()
	defer iter.Close()

	// Seek to the first key and iterate
	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		log.Printf("key: %s, value: %s\n", key.UserKey, string(value))
		count++
	}

	// Check for any errors during iteration
	if err := iter.Error(); err != nil {
		log.Fatalf("iterator failed with error: %v", err)
	}

	// Verification
	if count != 2 {
		log.Fatalf("expected to find 2 live keys, but found %d.", count)
	}
}
