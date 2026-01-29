package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	MemtableSizeThreshold = 4096 // 4 KB
)

type DBState struct {
	SSTableCounter int `json:"sstable_counter"`
}

type DB struct {
	mu           sync.RWMutex
	wal          *WAL
	mem          *Memtable
	immutableMem *Memtable

	dataDir        string
	sstableCounter int

	sequenceNum atomic.Uint64
}

// NewDB creates or opens a database at the specified path.
// It first replays all WALs to recover the state.
func NewDB(dir string) (*DB, error) {
	// Replay WAL to recover the state.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	var state DBState
	statePath := filepath.Join(dir, "state.json")
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("state file does not exist, initializing default state")
			state = DBState{SSTableCounter: 1}
		} else {
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		log.Printf("loaded state: sstable_counter is %d", state.SSTableCounter)
	}

	mem := NewMemtable()
	var maxSeqNum uint64 = 0

	// List all WAL files and sort them in order so that we replay in the order they were created.
	walFiles, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	sort.Strings(walFiles)

	activeWAL := filepath.Join(dir, "db.wal")
	walFiles = append(walFiles, activeWAL)

	for _, walPath := range walFiles {
		if _, err := os.Stat(walPath); os.IsNotExist(err) {
			continue
		}

		recoveredData, lastSeq, err := Replay(walPath)
		if err != nil {
			return nil, fmt.Errorf("failed to replay WAL %s: %w", walPath, err)
		}

		if lastSeq > maxSeqNum {
			maxSeqNum = lastSeq
		}

		for key, value := range recoveredData {
			mem.Put(key, value.Value)
		}
	}

	log.Printf("recovery complete, highest sequence number is %d", maxSeqNum)

	wal, err := NewWAL(activeWAL)
	if err != nil {
		return nil, err
	}

	db := &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		sstableCounter: state.SSTableCounter,
	}
	db.sequenceNum.Store(maxSeqNum)
	db.saveState()

	return db, nil
}

func (db *DB) saveState() error {
	state := DBState{
		SSTableCounter: db.sstableCounter,
	}

	data, err := json.MarshalIndent(state, "", "")
	if err != nil {
		return err
	}

	statePath := filepath.Join(db.dataDir, "state.json")
	return os.WriteFile(statePath, data, 0644)
}

func (db *DB) flushMemtable() {
	log.Printf("memtable is full, starting flush...")
	db.mu.Lock()
	if db.immutableMem != nil {
		db.mu.Unlock()
		return
	}

	// WAL rotation
	walPath := db.wal.file.Name()
	rotatedWALPath := fmt.Sprintf("%s/wal-%05d.log", db.dataDir, db.sstableCounter)
	db.wal.Close()

	if err := os.Rename(walPath, rotatedWALPath); err != nil {
		log.Printf("failed to rename WAL %s: %v", rotatedWALPath, err)
		db.mu.Unlock()
		return
	}

	newWAL, err := NewWAL(walPath)
	if err != nil {
		log.Printf("failed to open new WAL %s: %v", walPath, err)
		db.mu.Unlock()
		return
	}

	db.wal = newWAL
	db.immutableMem = db.mem
	db.mem = NewMemtable()
	db.mu.Unlock()

	go func(imm *Memtable, walToDelete string) {
		log.Printf("background flush: starting to write SSTable...")
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, db.sstableCounter)
		db.sstableCounter++

		itemCount := imm.data.Len()
		if err := WriteSSTable(sstablePath, uint(itemCount), imm.data.Front()); err != nil {
			log.Printf("failed to write SSTable: %v", err)
		}

		log.Printf("successfully flushed memtable to %s", sstablePath)

		db.mu.Lock()
		defer db.mu.Unlock()

		db.immutableMem = nil

		if err := db.saveState(); err != nil {
			log.Printf("failed to save state: %v", err)
			return
		}

		log.Printf("truncating WAL file...")
		if err := os.Remove(walToDelete); err != nil {
			log.Printf("failed to delete rotated WAL %s: %v", walToDelete, err)
		} else {
			log.Printf("background flush: deleted old WAL %s", walToDelete)
		}
	}(db.immutableMem, rotatedWALPath)
}

func (db *DB) Put(key, value []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{
		UserKey: string(key),
		SeqNum:  seqNum,
		Type:    OpTypePut,
	}
	entry := &LogEntry{
		Op:     OpPut,
		Key:    key,
		Value:  value,
		SeqNum: seqNum,
	}

	db.mu.RLock()
	wal := db.wal
	memtable := db.mem
	db.mu.RUnlock()

	if err := wal.Write(entry); err != nil {
		return err
	}

	memtable.Put(internalKey, value)

	if memtable.ApproximateSize() > MemtableSizeThreshold {
		db.flushMemtable()
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	mem := db.mem
	imm := db.immutableMem
	counter := db.sstableCounter
	db.mu.RUnlock()

	val, found := mem.Get(key)
	if found {
		if val == nil {
			// Found a delete tombstone
			return nil, false
		}
		return val, true
	}

	if imm != nil {
		val, found = imm.Get(key)
		if found {
			if val == nil {
				// Found a delete tombstone
				return nil, false
			}
			return val, true
		}
	}

	// Search key in newest to oldest SSTables
	for i := counter - 1; i >= 1; i-- {
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, i)
		reader, err := NewSSTableReader(sstablePath)
		if err != nil {
			log.Printf("failed to open reader %s: %v", sstablePath, err)
			continue
		}

		val, found, err = reader.Get(key)
		if err != nil {
			log.Printf("error reading SSTable %s: %v", sstablePath, err)
			continue
		}

		if found {
			if val == nil {
				return nil, false
			}
			return val, true
		}

		reader.Close()
	}

	return nil, false
}

func (db *DB) Delete(key []byte) error {
	seqNum := db.sequenceNum.Add(1)
	internalKey := InternalKey{
		UserKey: string(key),
		SeqNum:  seqNum,
		Type:    OpTypeDel,
	}
	entry := &LogEntry{
		Op:     OpDelete,
		Key:    key,
		SeqNum: seqNum,
	}

	db.mu.RLock()
	wal := db.wal
	memtable := db.mem
	db.mu.RUnlock()

	if err := wal.Write(entry); err != nil {
		return err
	}

	memtable.Put(internalKey, nil)
	if memtable.ApproximateSize() > MemtableSizeThreshold {
		db.flushMemtable()
	}

	return nil
}

func (db *DB) Close() error {
	return db.wal.Close()
}
