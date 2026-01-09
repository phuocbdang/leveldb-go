package main

import (
	"sync"

	"github.com/huandu/skiplist"
)

type Memtable struct {
	mu   sync.RWMutex
	data *skiplist.SkipList
	size int // Approximate size in bytes
}

func NewMemtable() *Memtable {
	return &Memtable{
		data: skiplist.New(skiplist.Bytes),
	}
}

func (m *Memtable) Put(key []byte, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldElem := m.data.Get(key); oldElem != nil {
		oldValue := oldElem.Value.([]byte)
		m.size -= len(key) + len(oldValue)
	}

	m.data.Set(key, value)
	m.size += len(key) + len(value)
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elem := m.data.Get(key)
	if elem != nil {
		return elem.Value.([]byte), true
	}
	return nil, false
}

func (m *Memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldElem := m.data.Remove(key); oldElem != nil {
		oldValue := oldElem.Value.([]byte)
		m.size -= len(key) + len(oldValue)
	}
}

func (m *Memtable) ApproximateSize() int {
	return m.size
}
