package kvstore

import (
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
)

const (
	NotFound string = "key not found"
	Empty           = ""
)

type KVResult struct {
	Ok    bool
	Value string
}

type KVStore interface {
	Get(key string) *string
	Put(key string, value string) bool
	Del(key string) bool
	Append(key string, suffix string) bool
	Close()
	MakeSnapshot() ([]byte, error)
	RestoreSnapshot([]byte)
}

func CreateStore(config config.Config) KVStore {
	if config.Store == "rocksdb" {
		return nil
	} else if config.Store == "mem" {
		return NewMemKVStore()
	} else {
		logger.Panic("no match kvstore")
		return nil
	}
}

func Execute(cmd *pb.Command, store KVStore) KVResult {
	switch cmd.Type {
	case pb.CommandType_GET:
		value := store.Get(cmd.Key)
		if value != nil {
			return KVResult{Ok: true, Value: *value}
		}
		return KVResult{Ok: false, Value: NotFound}

	case pb.CommandType_PUT:
		if store.Put(cmd.Key, cmd.Value) {
			return KVResult{Ok: true, Value: Empty}
		}
		return KVResult{Ok: false, Value: NotFound}

	case pb.CommandType_APPEND:
		if store.Append(cmd.Key, cmd.Value) {
			return KVResult{Ok: true, Value: Empty}
		}
		return KVResult{Ok: false, Value: NotFound}

	case pb.CommandType_DEL:
		if store.Del(cmd.Key) {
			return KVResult{Ok: true, Value: Empty}
		}
		return KVResult{Ok: false, Value: NotFound}

	default:
		panic("unsupported command type" + cmd.Type.String())
	}
}
