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

	"github.com/gofrs/flock"
)

const (
	SSTableCountThreshold = 3
	MemtableSizeThreshold = 4096 // 4 KB
)

type DBState struct {
	NextFileNumber int   `json:"next_file_number"`
	ActiveSSTables []int `json:"active_sstables"`
}

type DB struct {
	mu           sync.RWMutex
	wal          *WAL
	mem          *Memtable
	immutableMem *Memtable

	dataDir        string
	nextFileNumber int
	activeSSTables []int

	sequenceNum atomic.Uint64

	compactionInProgress bool

	dbLock *flock.Flock
}

// NewDB creates or opens a database at the specified path.
// It first replays all WALs to recover the state.
func NewDB(dir string) (*DB, error) {
	// Replay WAL to recover the state.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	lockPath := filepath.Join(dir, "lock")
	dbLock := flock.New(lockPath)
	locked, err := dbLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("database is locked by another process")
	}

	var state DBState
	statePath := filepath.Join(dir, "state.json")
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("state file does not exist, initializing default state")
			state = DBState{NextFileNumber: 1, ActiveSSTables: []int{}}
		} else {
			dbLock.Unlock()
			return nil, err
		}
	} else {
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		log.Printf("loaded state - next file number is %d, active SSTable is %v",
			state.NextFileNumber, state.ActiveSSTables)
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
			dbLock.Unlock()
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
		dbLock.Unlock()
		return nil, err
	}

	db := &DB{
		wal:            wal,
		mem:            mem,
		dataDir:        dir,
		nextFileNumber: state.NextFileNumber,
		activeSSTables: state.ActiveSSTables,
		dbLock:         dbLock,
	}
	db.sequenceNum.Store(maxSeqNum)
	db.saveState()

	return db, nil
}

func (db *DB) saveState() error {
	state := DBState{
		NextFileNumber: db.nextFileNumber,
		ActiveSSTables: db.activeSSTables,
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
	sstNum := db.nextFileNumber
	db.nextFileNumber++
	walPath := db.wal.file.Name()
	rotatedWALPath := fmt.Sprintf("%s/wal-%05d.log", db.dataDir, sstNum)
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

	go func(imm *Memtable, walToDelete string, sstNum int) {
		log.Printf("background flush: starting to write SSTable %d...", sstNum)
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, sstNum)

		itemCount := imm.data.Len()
		if err := WriteSSTable(sstablePath, uint(itemCount), imm.data.Front()); err != nil {
			log.Printf("failed to write SSTable: %v", err)
		}

		log.Printf("successfully flushed memtable to %s", sstablePath)

		db.mu.Lock()
		defer db.mu.Unlock()

		db.immutableMem = nil
		db.activeSSTables = append(db.activeSSTables, sstNum)
		sort.Ints(db.activeSSTables)

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

		if len(db.activeSSTables) >= SSTableCountThreshold && !db.compactionInProgress {
			go db.compact()
		}
	}(db.immutableMem, rotatedWALPath, sstNum)
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
	activeTables := db.activeSSTables
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
	for i := len(activeTables) - 1; i >= 0; i-- {
		sstNum := activeTables[i]
		sstablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, sstNum)
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
	if db.dbLock != nil {
		if err := db.dbLock.Unlock(); err != nil {
			log.Printf("failed to unlock database: %v", err)
		}
	}

	return db.wal.Close()
}
