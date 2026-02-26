# LevelDB Go

A from-scratch implementation of a LevelDB-inspired key-value storage engine in Go, featuring WAL, Memtables, SSTables, and compaction.

This project is built for learning purposes to deeply understand how modern embedded databases work under the hood.

## Overview

LevelDB (originally by Google) is a widely studied storage engine that powers many production systems. This implementation reproduces its core architecture:

- **Durability** via a Write-Ahead Log (WAL)
- **Fast writes** via an in-memory Memtable backed by a skip list
- **Persistent storage** via Sorted String Tables (SSTables)
- **Read acceleration** via Bloom filters, a block cache, and a table cache
- **Space reclamation** via background compaction
- **Crash recovery** by replaying WAL files on startup
- **Process safety** via a file lock

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                      DB (db.go)                     │
│                                                     │
│  Put / Get / Delete / NewIterator / Close           │
└──────────┬───────────────────────────┬──────────────┘
           │ Write Path                │ Read Path
           ▼                           ▼
┌──────────────────┐       ┌───────────────────────────┐
│   WAL (wal.go)   │       │  1. Memtable (active)     │
│ Write-Ahead Log  │       │  2. Memtable (immutable)  │
│ CRC32 checksums  │       │  3. SSTables (newest→old) │
└──────────────────┘       │     └─ Bloom Filter       │
           │               │     └─ Index Block        │
           ▼               │     └─ Data Blocks        │
┌──────────────────┐       └───────────────────────────┘
│  Memtable        │
│  (skip list)     │──── full ────► Flush to SSTable
└──────────────────┘                      │
                                          ▼
                               ┌──────────────────────┐
                               │  SSTable (sstable.go)│
                               │  .sst file on disk   │
                               └──────────────────────┘
                                          │
                               count >= 3 │
                                          ▼
                               ┌──────────────────────┐
                               │ Compaction           │
                               │ (compaction.go)      │
                               │ k-way merge + dedup  │
                               └──────────────────────┘
```

---

## Core Components

### 1. Write-Ahead Log (`wal.go`)

Every write (`Put`/`Delete`) is first appended to the WAL before touching memory. This guarantees durability: if the process crashes, the WAL can be replayed on the next startup.

**Binary record format:**
```
[Checksum (4B)] [SeqNum (8B)] [KeySize (4B)] [ValueSize (4B)] [Op (1B)] [Key] [Value]
```

- Each record is protected by a **CRC32 checksum** to detect corruption.
- The WAL is `fsync`'d after every write to flush data from the OS buffer to persistent storage.
- When the Memtable is flushed to disk, its associated WAL file is rotated (renamed) and deleted after the SSTable is confirmed written.

### 2. Memtable (`memtable.go`)

An in-memory, ordered map that absorbs all incoming writes. It is backed by a **skip list** for O(log n) insertions and lookups.

- Keys are stored as `InternalKey` = `(UserKey, SequenceNumber, OperationType)`.
- **Deletes** are not physical removals. They insert a **tombstone** (`OpTypeDel`) which masks the old value.
- Once the Memtable exceeds **4 MB**, it is frozen into an **immutable Memtable** and a new active one is created. A background goroutine flushes the immutable Memtable to an SSTable.

### 3. Internal Key (`internal_key.go`)

Each key stored internally has three fields:

```go
type InternalKey struct {
    UserKey string
    SeqNum  uint64  // Monotonically increasing, higher = newer
    Type    OpType  // OpTypePut or OpTypeDel
}
```

The comparator sorts by `UserKey` ascending, then by `SeqNum` **descending** — so the most recent version of a key always appears first during iteration.

### 4. SSTable (`sstable.go`)

The on-disk, immutable, sorted file format. Each SSTable is written once (when a Memtable is flushed) and never modified.

**File layout:**
```
┌─────────────────────────────────────┐
│  Data Block 0  (≤4KB of key-values) │
│  Data Block 1                       │
│  ...                                │
├─────────────────────────────────────┤
│  Filter Block  (Bloom filter)       │
├─────────────────────────────────────┤
│  Index Block   (offsets per block)  │
├─────────────────────────────────────┤
│  Footer        (index/filter locs)  │
│  Footer Size   (4 bytes)            │
└─────────────────────────────────────┘
```

**Reading a key:**
1. **Bloom filter check** — if the key is definitely not in this file, skip it entirely (no I/O).
2. **Binary search the index** — find which 4KB data block might contain the key.
3. **Scan the data block** — read only that block from disk (or cache) and scan for the key.

### 5. Block Cache & Table Cache (`db.go`)

Two LRU caches reduce disk I/O:

| Cache | Capacity | Stores |
|---|---|---|
| **Table Cache** | 128 entries | Open `SSTableReader` handles (avoids reopening files) |
| **Block Cache** | 8 MB | Raw data block bytes (avoids re-reading the same 4KB block) |

Cache keys for the block cache are `"fileNum:blockOffset"` — unique across all SSTable files.

### 6. Compaction (`compaction.go`)

When the number of SSTable files reaches **3**, a background goroutine starts compaction:

1. All existing SSTable files are opened as streaming iterators.
2. A **min-heap k-way merge** combines them into a single sorted stream.
3. For duplicate user keys, only the **latest version** is kept. Tombstones (`OpTypeDel`) are dropped.
4. The merged result is written to a new `.tmp` SSTable, then atomically renamed.
5. Old SSTable files are removed; cache entries for them are invalidated.

This keeps the number of files bounded and reclaims space used by deleted/overwritten keys.

### 7. Iterator (`db_iterator.go`)

`db.NewIterator()` returns a **MergingIterator** that presents a unified, sorted view across all live data sources:

```
MergingIterator
  ├── Memtable iterator        (newest)
  ├── Immutable Memtable iter  (if flushing)
  ├── SSTable[N] iterator      (newer)
  └── SSTable[0] iterator      (oldest)
```

The merge uses a min-heap internally. Duplicate user keys (older versions or tombstones) are skipped so the caller always sees the latest live value.

---

## Write & Read Paths

### Write Path (`Put`)
```
db.Put(key, value)
  1. Atomically increment sequence number
  2. Write LogEntry to WAL  ──► fsync to disk
  3. Insert InternalKey into Memtable (skip list)
  4. If Memtable.size > 4MB:
       a. Rotate WAL file
       b. Freeze Memtable → immutableMem
       c. Create new active Memtable
       d. Goroutine: flush immutableMem → SSTable on disk
       e. If SSTable count ≥ 3 → goroutine: compact
```

### Read Path (`Get`)
```
db.Get(key)
  1. Search active Memtable            (O(log n))
  2. Search immutable Memtable, if any (O(log n))
  3. For each SSTable, newest → oldest:
       a. Bloom filter (skip file if key definitely absent)
       b. Binary search index block
       c. Scan data block (from cache or disk)
  → Return first match found (or nil if tombstone)
```

### Crash Recovery
```
NewDB(dir)
  1. Acquire file lock (prevents double-open)
  2. Load state.json (SSTable list, next file number)
  3. Replay all WAL files in chronological order
       → Rebuild Memtable from WAL entries
       → Track highest sequence number
  4. Open new WAL for incoming writes
```

---

## Usage Example

```go
package main

import "log"

func main() {
    db, err := NewDB("./mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Write
    db.Put([]byte("apple"),  []byte("red"))
    db.Put([]byte("banana"), []byte("yellow"))
    db.Put([]byte("apple"),  []byte("green")) // overwrites previous value

    // Delete
    db.Delete([]byte("banana"))

    // Point lookup
    val, ok := db.Get([]byte("apple"))
    if ok {
        log.Printf("apple = %s", val) // "green"
    }

    val, ok = db.Get([]byte("banana"))
    // ok == false, banana was deleted

    // Range scan
    iter := db.NewIterator()
    defer iter.Close()

    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        log.Printf("%s = %s", iter.Key().UserKey, iter.Value())
    }

    if err := iter.Error(); err != nil {
        log.Fatal(err)
    }
}
```

---

## Project Structure

```
leveldb-go/
├── db.go            # Core DB struct: Put, Get, Delete, Close, NewIterator, flush logic
├── wal.go           # Write-Ahead Log: Write, Replay, CRC32 checksums
├── memtable.go      # In-memory skip list: Put, Get, iterator
├── internal_key.go  # InternalKey type and comparator
├── sstable.go       # SSTable writer and reader: data/index/filter/footer blocks
├── compaction.go    # K-way merge compaction using min-heap
├── db_iterator.go   # Iterator interface and MergingIterator
└── main.go          # Demo / integration test
```

---

## Key Design Decisions

**Why a skip list for the Memtable?**
Skip lists provide O(log n) average-case for insert and lookup with simpler implementation than a red-black tree. They also support ordered iteration natively, which is required for flushing a sorted SSTable.

**Why tombstones instead of in-place deletes?**
SSTables are immutable. A delete cannot remove data from an existing SSTable. Instead, a tombstone is written to the Memtable (and eventually to a newer SSTable). During compaction, when the tombstone is the newest entry for a key and all older copies are also being merged, the key is finally erased.

**Why sequence numbers?**
Multiple versions of the same user key can exist across different SSTables (written at different times). Sequence numbers create a total ordering of writes. The internal key comparator sorts by user key first, then by sequence number descending, so the most recent write is always found first.

**Why a two-level cache?**
The table cache avoids the overhead of re-opening file descriptors on every lookup. The block cache avoids re-reading the same 4KB chunk from disk. Together, hot data that fits in the 8 MB block cache is served entirely from memory.

**Why atomic file rename for compaction output?**
Writing to a `.tmp` file and then renaming it to the final `.sst` path is an atomic operation on most filesystems. This ensures a reader never sees a partially written SSTable file, even if the process crashes mid-compaction.

---

## Running

```bash
go run .
```

The `main.go` demo writes a few keys, deletes one, and iterates over the result, verifying exactly 2 live keys remain.

---

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/huandu/skiplist` | Skip list data structure for Memtable |
| `github.com/bits-and-blooms/bloom/v3` | Bloom filter for SSTable fast-negative lookups |
| `github.com/hashicorp/golang-lru/v2` | LRU cache for table cache and block cache |
| `github.com/gofrs/flock` | Cross-platform file locking |
