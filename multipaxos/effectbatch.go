package multipaxos

import (
	"strings"
	"sync/atomic"

	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
)

type RequestKind int

const (
	WriteRequest RequestKind = iota
	ReadRequest
)

type EffectNodeKind int

const (
	ExplicitNode EffectNodeKind = iota
	WriteSummaryNode
)

type SummaryBaseKind int

const (
	SummaryFromStore SummaryBaseKind = iota
	SummaryFromPut
)

type PendingRequest struct {
	ReqID    int64
	Kind     RequestKind
	ClientID int64
	Commands []*pb.Command
	Callback chan Result
}

type PendingCommandRef struct {
	Req      *PendingRequest
	CmdIndex int
}

type EffectNode struct {
	Kind         EffectNodeKind
	Key          string
	Command      *pb.Command
	BaseKind     SummaryBaseKind
	PutValue     string
	AppendSuffix strings.Builder
	Members      []PendingCommandRef
	Seq          int64
}

type EffectBatch struct {
	Nodes     []*EffectNode
	TailByKey map[string]*EffectNode
	Seq       int64
	HasReads  bool
}

func classifyRequest(commands []*pb.Command) RequestKind {
	if len(commands) == 0 {
		return WriteRequest
	}
	for _, cmd := range commands {
		if cmd.Type != pb.CommandType_GET {
			return WriteRequest
		}
	}
	return ReadRequest
}

func (p *Multipaxos) nextRequestID() int64 {
	return atomic.AddInt64(&p.nextReqID, 1)
}

func newExplicitNode(cmd *pb.Command, req *PendingRequest, cmdIndex int, seq int64) *EffectNode {
	return &EffectNode{
		Kind:    ExplicitNode,
		Key:     cmd.Key,
		Command: cmd,
		Members: []PendingCommandRef{{
			Req:      req,
			CmdIndex: cmdIndex,
		}},
		Seq: seq,
	}
}

func canAbsorb(tail *EffectNode, cmd *pb.Command) bool {
	if tail == nil {
		return false
	}
	if tail.Kind != WriteSummaryNode {
		return false
	}
	return cmd.Type == pb.CommandType_PUT || cmd.Type == pb.CommandType_APPEND
}

func absorb(node *EffectNode, req *PendingRequest, cmdIndex int, cmd *pb.Command) {
	switch cmd.Type {
	case pb.CommandType_PUT:
		node.BaseKind = SummaryFromPut
		node.PutValue = cmd.Value
		node.AppendSuffix.Reset()
	case pb.CommandType_APPEND:
		node.AppendSuffix.WriteString(cmd.Value)
	}
	node.Members = append(node.Members, PendingCommandRef{
		Req:      req,
		CmdIndex: cmdIndex,
	})
}

func newNodeFromCommand(cmd *pb.Command, req *PendingRequest, cmdIndex int, seq int64) *EffectNode {
	switch cmd.Type {
	case pb.CommandType_PUT:
		return &EffectNode{
			Kind:     WriteSummaryNode,
			Key:      cmd.Key,
			BaseKind: SummaryFromPut,
			PutValue: cmd.Value,
			Members: []PendingCommandRef{{
				Req:      req,
				CmdIndex: cmdIndex,
			}},
			Seq: seq,
		}
	case pb.CommandType_APPEND:
		node := &EffectNode{
			Kind:     WriteSummaryNode,
			Key:      cmd.Key,
			BaseKind: SummaryFromStore,
			PutValue: "",
			Members: []PendingCommandRef{{
				Req:      req,
				CmdIndex: cmdIndex,
			}},
			Seq: seq,
		}
		node.AppendSuffix.WriteString(cmd.Value)
		return node
	default:
		return newExplicitNode(cmd, req, cmdIndex, seq)
	}
}

func NewEffectBatch(capacity int) *EffectBatch {
	return &EffectBatch{
		Nodes:     make([]*EffectNode, 0, capacity),
		TailByKey: make(map[string]*EffectNode, capacity),
		Seq:       0,
		HasReads:  false,
	}
}

func (eb *EffectBatch) Add(req *PendingRequest) {
	if req.Kind == ReadRequest {
		eb.HasReads = true
		return
	}

	// Only single-command requests participate in condensation.
	if len(req.Commands) != 1 {
		for i, cmd := range req.Commands {
			if cmd.Type == pb.CommandType_GET {
				eb.HasReads = true
				continue
			}
			node := newExplicitNode(cmd, req, i, eb.Seq)
			eb.Seq++
			eb.Nodes = append(eb.Nodes, node)
			eb.TailByKey[cmd.Key] = node
		}
		return
	}

	cmd := req.Commands[0]
	if cmd.Type == pb.CommandType_GET {
		eb.HasReads = true
		return
	}

	tail := eb.TailByKey[cmd.Key]
	if canAbsorb(tail, cmd) {
		absorb(tail, req, 0, cmd)
		return
	}

	node := newNodeFromCommand(cmd, req, 0, eb.Seq)
	eb.Seq++
	eb.Nodes = append(eb.Nodes, node)
	eb.TailByKey[cmd.Key] = node
}

func materializeEffectBatch(eb *EffectBatch) []*pb.Command {
	commands := make([]*pb.Command, 0, len(eb.Nodes)+1)

	if eb.HasReads {
		commands = append(commands, &pb.Command{
			Type:     pb.CommandType_NOOP,
			ClientId: []int64{-1},
		})
	}

	for _, node := range eb.Nodes {
		switch node.Kind {
		case ExplicitNode:
			commands = append(commands, node.Command)

		case WriteSummaryNode:
			clientIDs := make([]int64, 0, len(node.Members))
			for _, member := range node.Members {
				clientIDs = append(clientIDs, member.Req.ClientID)
			}
			if len(clientIDs) == 0 {
				continue
			}
			if node.BaseKind == SummaryFromPut {
				commands = append(commands, &pb.Command{
					Type:     pb.CommandType_PUT,
					Key:      node.Key,
					Value:    node.PutValue + node.AppendSuffix.String(),
					ClientId: clientIDs,
				})
			} else {
				commands = append(commands, &pb.Command{
					Type:     pb.CommandType_APPEND,
					Key:      node.Key,
					Value:    node.AppendSuffix.String(),
					ClientId: clientIDs,
				})
			}
		}
	}

	return commands
}
