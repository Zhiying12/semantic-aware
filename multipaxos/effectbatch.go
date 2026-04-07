package multipaxos

import (
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
	AppendSuffix string
	Members      []PendingCommandRef
	Seq          int64
}

type EffectBatch struct {
	Nodes     []*EffectNode
	TailByKey map[string]*EffectNode
}

func classifyRequest(commands []*pb.Command) RequestKind {
	if len(commands) == 1 && commands[0].Type == pb.CommandType_GET {
		return ReadRequest
	}
	return WriteRequest
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
		node.AppendSuffix = ""
	case pb.CommandType_APPEND:
		node.AppendSuffix += cmd.Value
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
			Kind:         WriteSummaryNode,
			Key:          cmd.Key,
			BaseKind:     SummaryFromPut,
			PutValue:     cmd.Value,
			AppendSuffix: "",
			Members: []PendingCommandRef{{
				Req:      req,
				CmdIndex: cmdIndex,
			}},
			Seq: seq,
		}
	case pb.CommandType_APPEND:
		return &EffectNode{
			Kind:         WriteSummaryNode,
			Key:          cmd.Key,
			BaseKind:     SummaryFromStore,
			PutValue:     "",
			AppendSuffix: cmd.Value,
			Members: []PendingCommandRef{{
				Req:      req,
				CmdIndex: cmdIndex,
			}},
			Seq: seq,
		}
	default:
		return newExplicitNode(cmd, req, cmdIndex, seq)
	}
}

func buildEffectBatch(batch []*PendingRequest) *EffectBatch {
	eb := &EffectBatch{
		Nodes:     make([]*EffectNode, 0),
		TailByKey: make(map[string]*EffectNode),
	}
	var seq int64

	for _, req := range batch {
		// Only single-command requests participate in condensation.
		if len(req.Commands) != 1 {
			for i, cmd := range req.Commands {
				node := newExplicitNode(cmd, req, i, seq)
				seq++
				eb.Nodes = append(eb.Nodes, node)
				eb.TailByKey[cmd.Key] = node
			}
			continue
		}

		cmd := req.Commands[0]
		tail := eb.TailByKey[cmd.Key]
		if canAbsorb(tail, cmd) {
			absorb(tail, req, 0, cmd)
			continue
		}

		node := newNodeFromCommand(cmd, req, 0, seq)
		seq++
		eb.Nodes = append(eb.Nodes, node)
		eb.TailByKey[cmd.Key] = node
	}

	return eb
}

func materializeEffectBatch(eb *EffectBatch) []*pb.Command {
	commands := make([]*pb.Command, 0, len(eb.Nodes))

	for _, node := range eb.Nodes {
		switch node.Kind {
		case ExplicitNode:
			commands = append(commands, node.Command)

		case WriteSummaryNode:
			var clientIDs []int64
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
					Value:    node.PutValue + node.AppendSuffix,
					ClientId: clientIDs,
				})
			} else {
				commands = append(commands, &pb.Command{
					Type:     pb.CommandType_APPEND,
					Key:      node.Key,
					Value:    node.AppendSuffix,
					ClientId: clientIDs,
				})
			}
		}
	}

	return commands
}
