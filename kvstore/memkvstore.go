package kvstore

import (
	"bytes"
	"encoding/gob"
	"sync"

	logger "github.com/sirupsen/logrus"
)

type MemKVStore struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewMemKVStore() *MemKVStore {
	s := MemKVStore{
		store: make(map[string]string),
	}
	return &s
}

func (s *MemKVStore) Get(key string) *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, ok := s.store[key]; ok {
		return &value
	} else {
		return nil
	}
}

func (s *MemKVStore) Put(key string, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
	return true
}

func (s *MemKVStore) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.store[key]; ok {
		delete(s.store, key)
		return true
	} else {
		return false
	}
}

func (s *MemKVStore) Append(key string, suffix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value, ok := s.store[key]; ok {
		s.store[key] = value + suffix
	} else {
		s.store[key] = suffix
	}
	return true
}

func (s *MemKVStore) BatchRead(keys []string) []*string {
	results := make([]*string, len(keys))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, key := range keys {
		if value, ok := s.store[key]; ok {
			results[i] = &value
		} else {
			results[i] = nil
		}
	}
	return results
}

func (s *MemKVStore) BatchWrite(keys []string, values []string) bool {
	if len(keys) != len(values) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, key := range keys {
		s.store[key] = values[i]
	}
	return true
}

func (s *MemKVStore) Close() {}

func (s *MemKVStore) MakeSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	buffer := &bytes.Buffer{}
	err := gob.NewEncoder(buffer).Encode(s.store)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *MemKVStore) RestoreSnapshot(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer := bytes.NewBuffer(data)
	err := gob.NewDecoder(buffer).Decode(&s.store)
	if err != nil {
		logger.Error(err)
		return
	}
}
