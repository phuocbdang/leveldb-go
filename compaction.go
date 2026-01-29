package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/huandu/skiplist"
)

type minHeap []*heapItem

func (h minHeap) Len() int {
	return len(h)
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(*heapItem))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]

	return item
}

func (h minHeap) Less(i, j int) bool {
	return NewInternalKeyComparator().Compare(h[i].key, h[j].key) < 0
}

type heapItem struct {
	key      InternalKey
	value    []byte
	iterator *sstableIterator
}

type sstableIterator struct {
	file   *os.File
	reader *bufio.Reader
	key    InternalKey
	value  []byte
	err    error
}

func (it *sstableIterator) Close() error {
	if it.file != nil {
		return it.file.Close()
	}
	return nil
}

// newSSTableFileIterator creates an iterator that streams from a file path.
func newSSTableFileIterator(path string) (*sstableIterator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &sstableIterator{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (it *sstableIterator) Next() bool {
	var keySize uint32

	if err := binary.Read(it.reader, binary.LittleEndian, &keySize); err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	var valueSize uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &valueSize); err != nil {
		it.err = err
		return false
	}

	keyBytes := make([]byte, keySize)
	if _, err := io.ReadFull(it.reader, keyBytes); err != nil {
		it.err = err
		return false
	}
	if err := gob.NewDecoder(bytes.NewReader(keyBytes)).Decode(&it.key); err != nil {
		it.err = err
		return false
	}

	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(it.reader, valueBytes); err != nil {
		it.err = err
		return false
	}
	it.value = valueBytes

	return true
}

// MergeSSTables compacts multiple SSTable into a single new one.
func MergeSSTables(paths []string, outputPath string) error {
	var iterators []*sstableIterator
	for _, path := range paths {
		it, err := newSSTableFileIterator(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			// Close any already opened iterators before returning
			for _, openIt := range iterators {
				openIt.Close()
			}
			return err
		}
		iterators = append(iterators, it)
	}

	// Ensure all iterators are closed when done
	defer func() {
		for _, it := range iterators {
			it.Close()
		}
	}()

	h := &minHeap{}
	heap.Init(h)

	for _, it := range iterators {
		if it.Next() {
			heap.Push(h, &heapItem{
				key:      it.key,
				value:    it.value,
				iterator: it,
			})
		}
	}

	list := skiplist.New(NewInternalKeyComparator())
	var lastUserKey string
	var itemCount uint

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)
		// Skip all older events
		if item.key.UserKey != lastUserKey {
			if item.key.Type == OpTypePut {
				list.Set(item.key, item.value)
				itemCount++
			}
			lastUserKey = item.key.UserKey
		}
		if item.iterator.Next() {
			heap.Push(h, &heapItem{
				key:      item.iterator.key,
				value:    item.iterator.value,
				iterator: item.iterator,
			})
		}
	}

	if list.Len() == 0 {
		// It's possible for a compaction to result in no keys if all keys
		// were deleted. In this case, we don't create an empty SSTable.
		return nil
	}

	return WriteSSTable(outputPath, itemCount, list.Front())
}

func (db *DB) compact() {
	defer db.wg.Done()
	log.Println("starting compaction...")

	db.mu.Lock()
	db.compactionInProgress = true
	tablesToCompact := make([]int, len(db.activeSSTables))
	copy(tablesToCompact, db.activeSSTables)
	outputNum := db.nextFileNumber
	db.nextFileNumber++
	db.mu.Unlock()

	var pathsToCompact []string
	for _, num := range tablesToCompact {
		pathsToCompact = append(pathsToCompact, fmt.Sprintf("%s/%05d.sst", db.dataDir, num))
	}
	newSSTablePath := fmt.Sprintf("%s/%05d.sst", db.dataDir, outputNum)
	tmpPath := newSSTablePath + ".tmp"

	if err := MergeSSTables(pathsToCompact, tmpPath); err != nil {
		log.Printf("compaction failed: %v", err)
		return
	}

	if err := os.Rename(tmpPath, newSSTablePath); err != nil {
		log.Printf("compaction failed during file rename: %v", err)
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	newActiveTables := []int{outputNum}
	isCompacted := make(map[int]bool)
	for _, num := range tablesToCompact {
		isCompacted[num] = true
	}

	// Check the *current* activeSSTables list for any new files.
	for _, num := range db.activeSSTables {
		if !isCompacted[num] {
			newActiveTables = append(newActiveTables, num)
		}
	}

	db.activeSSTables = newActiveTables
	sort.Ints(db.activeSSTables)

	if err := db.saveState(); err != nil {
		log.Printf("failed to save state after compaction: %v", err)
		return
	}

	log.Printf("compaction completed successfully to %s", newSSTablePath)

	db.compactionInProgress = false

	db.wg.Add(1)
	go func(pathsToCompact []string) {
		defer db.wg.Done()
		for _, path := range pathsToCompact {
			if err := os.Remove(path); err != nil {
				log.Printf("failed to remove old SSTable %s after compaction: %v", path, err)
			}
		}
	}(pathsToCompact)
}
