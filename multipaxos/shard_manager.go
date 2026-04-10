package multipaxos

import (
	"hash/fnv"
)

// ShardManager handles mapping of keys to virtual shards and virtual shards to logs.
type ShardManager struct {
	numVirtualShards int
	numLogs          int
	shardToLog       []int32 // Dummy mapping for now
}

// NewManager creates a new shard manager.
func NewShardManager(numVirtualShards, numLogs int) *ShardManager {
	if numLogs <= 0 {
		numLogs = 1
	}
	if numVirtualShards <= 0 {
		numVirtualShards = 1
	}
	m := &ShardManager{
		numVirtualShards: numVirtualShards,
		numLogs:          numLogs,
		shardToLog:       make([]int32, numVirtualShards),
	}

	// Dummy mapping: shard `i` maps to log `i % numLogs`
	for i := 0; i < numVirtualShards; i++ {
		m.shardToLog[i] = int32(i % numLogs)
	}

	return m
}

func (m *ShardManager) getVirtualShard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % m.numVirtualShards
}

// GetLogID returns the log ID for a given key.
func (m *ShardManager) GetLogID(key string) int32 {
	if m.numLogs <= 1 {
		return 0
	}
	shardID := m.getVirtualShard(key)
	return m.shardToLog[shardID]
}
