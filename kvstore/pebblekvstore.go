package kvstore

import (
	"bytes"
	"encoding/gob"

	"github.com/cockroachdb/pebble"
	logger "github.com/sirupsen/logrus"
)

type PebbleKVStore struct {
	db *pebble.DB
}

func NewPebbleKVStore(dbPath string) *PebbleKVStore {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		logger.Fatalf("failed to open pebble db at %s: %v", dbPath, err)
	}
	return &PebbleKVStore{
		db: db,
	}
}

func (s *PebbleKVStore) Get(key string) *string {
	val, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return nil
	}
	defer closer.Close()
	v := string(val)
	return &v
}

func (s *PebbleKVStore) Put(key string, value string) bool {
	err := s.db.Set([]byte(key), []byte(value), pebble.Sync)
	if err != nil {
		logger.Errorf("failed to put key %s: %v", key, err)
		return false
	}
	return true
}

func (s *PebbleKVStore) Del(key string) bool {
	// Read the key first to return true only if it actually exists,
	// preserving the same semantics as MemKVStore.
	_, closer, err := s.db.Get([]byte(key))
	if err != nil {
		return false
	}
	closer.Close()

	err = s.db.Delete([]byte(key), pebble.Sync)
	if err != nil {
		logger.Errorf("failed to delete key %s: %v", key, err)
		return false
	}
	return true
}

func (s *PebbleKVStore) Close() {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			logger.Errorf("failed to close pebble db: %v", err)
		}
	}
}

func (s *PebbleKVStore) MakeSnapshot() ([]byte, error) {
	snapshotMap := make(map[string]string)
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		snapshotMap[string(iter.Key())] = string(iter.Value())
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	
	buffer := &bytes.Buffer{}
	err = gob.NewEncoder(buffer).Encode(snapshotMap)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *PebbleKVStore) RestoreSnapshot(data []byte) {
	buffer := bytes.NewBuffer(data)
	var snapshotMap map[string]string
	err := gob.NewDecoder(buffer).Decode(&snapshotMap)
	if err != nil {
		logger.Error(err)
		return
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	// Apply all keys from the snapshot
	for k, v := range snapshotMap {
		batch.Set([]byte(k), []byte(v), nil)
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		logger.Error(err)
	}
}
