package multipaxos

import (
	"sync"

	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
)

const (
	IdBits               = 0xff
	RoundIncrement       = IdBits + 1
	MaxNumPeers    int64 = 0xf
	ChunkSize            = 64 * 1024
)

type RpcPeer struct {
	Id   int64
	Stub pb.MultiPaxosRPCClient
}

func NewRpcPeer(id int64, stub pb.MultiPaxosRPCClient) *RpcPeer {
	peer := &RpcPeer{
		Id:   id,
		Stub: stub,
	}
	return peer
}

type RpcPeerList struct {
	sync.RWMutex
	List []*RpcPeer
}

type ResultType int

const (
	Ok ResultType = iota
	Retry
	SomeElseLeader
)

type Result struct {
	Type   ResultType
	Leader int64
	Value  string
}

func ExtractLeaderId(ballot int64) int64 {
	return ballot & IdBits
}

func IsLeader(ballot int64, id int64) bool {
	return ExtractLeaderId(ballot) == id
}

func IsSomeoneElseLeader(ballot int64, id int64) bool {
	return !IsLeader(ballot, id) && ExtractLeaderId(ballot) < MaxNumPeers
}

type PrepareState struct {
	NumRpcs      int
	NumOks       int
	MaxLastIndex map[int32]int64
	Mu           sync.Mutex
	Cv           *sync.Cond
}

func NewPrepareState() *PrepareState {
	prepareState := &PrepareState{
		NumRpcs:      0,
		NumOks:       0,
		MaxLastIndex: make(map[int32]int64),
	}
	prepareState.Cv = sync.NewCond(&prepareState.Mu)
	return prepareState
}

type AcceptState struct {
	NumRpcs int
	NumOks  int
	Mu      sync.Mutex
	Cv      *sync.Cond
}

func NewAcceptState() *AcceptState {
	acceptState := &AcceptState{
		NumRpcs: 0,
		NumOks:  0,
	}
	acceptState.Cv = sync.NewCond(&acceptState.Mu)
	return acceptState
}

type CommitState struct {
	NumRpcs         int
	NumOks          int
	MinLastExecuted map[int32]int64
	Mu              sync.Mutex
	Cv              *sync.Cond
}

func NewCommitState(minLastExecuted map[int32]int64) *CommitState {
	commitState := &CommitState{
		NumRpcs:         0,
		NumOks:          0,
		MinLastExecuted: minLastExecuted,
	}
	commitState.Cv = sync.NewCond(&commitState.Mu)
	return commitState
}

type ReplayState struct {
	NumRpcs       int
	NumOks        int
	RecoveredLogs map[int32]map[int64]*pb.Instance
	Mu            sync.Mutex
	Cv            *sync.Cond
}

func NewReplayState() *ReplayState {
	replayState := &ReplayState{
		NumRpcs:       1,
		NumOks:        1,
		RecoveredLogs: make(map[int32]map[int64]*pb.Instance),
	}
	replayState.Cv = sync.NewCond(&replayState.Mu)
	return replayState
}
