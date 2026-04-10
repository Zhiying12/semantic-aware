package replicant

import (
	"net"
	"strconv"
	"strings"

	"github.com/psu-csl/replicated-store/go/config"
	"github.com/psu-csl/replicated-store/go/kvstore"
	consensusLog "github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
	logger "github.com/sirupsen/logrus"
)

type Replicant struct {
	id            int64
	logs          []*consensusLog.Log
	multipaxos    *multipaxos.Multipaxos
	ipPort        string
	acceptor      net.Listener
	clientManager *ClientManager
}

func NewReplicant(config config.Config, join bool) *Replicant {
	r := &Replicant{
		id:     config.Id,
		ipPort: config.Peers[config.Id],
	}
	if config.NumLogs == 0 {
		config.NumLogs = 1
	}
	r.logs = make([]*consensusLog.Log, config.NumLogs)
	for i := 0; i < config.NumLogs; i++ {
		cfg := config
		cfg.DbPath = config.DbPath + "_" + strconv.Itoa(i)
		r.logs[i] = consensusLog.NewLog(kvstore.CreateStore(cfg))
	}
	r.multipaxos = multipaxos.NewMultipaxos(r.logs, config, join)
	r.clientManager = NewClientManager(r.id, int64(len(config.Peers)), r.multipaxos)
	return r
}

func (r *Replicant) Start() {
	r.multipaxos.Start()
	r.StartExecutorThread()
	r.StartServer()
}

func (r *Replicant) Stop() {
	r.StopServer()
	r.StopExecutorThread()
	r.multipaxos.Stop()
}

func (r *Replicant) StartServer() {
	pos := strings.Index(r.ipPort, ":")
	if pos == -1 {
		panic("no separator : in the acceptor port")
	}
	pos += 1
	port, err := strconv.Atoi(r.ipPort[pos:])
	if err != nil {
		panic("parsing acceptor port failed")
	}
	port += 1

	acceptor, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		logger.Fatalln(err)
	}
	logger.Infof("%v starting server at port %v\n", r.id, port)
	r.acceptor = acceptor
	r.AcceptClient()
}

func (r *Replicant) StopServer() {
	r.acceptor.Close()
	r.clientManager.StopAll()
}

func (r *Replicant) StartExecutorThread() {
	logger.Infof("%v starting executor thread\n", r.id)
	for i := 0; i < len(r.logs); i++ {
		go r.executorThread(i)
	}
}

func (r *Replicant) StopExecutorThread() {
	logger.Infof("%v stopping executor thread\n", r.id)
	for i := 0; i < len(r.logs); i++ {
		r.logs[i].Stop()
	}
}

func (r *Replicant) StartRpcServer() {
	r.multipaxos.StartRPCServer()
}

func (r *Replicant) executorThread(logId int) {
	for {
		instance := r.logs[logId].ReadInstance()
		if instance == nil {
			break
		}
		if len(instance.Commands) == 0 {
			continue
		}

		if instance.Commands[0].Type == pb.CommandType_ADDNODE ||
			instance.Commands[0].Type == pb.CommandType_DELNODE {
			r.multipaxos.Reconfigure(instance.Commands[0])
			client := r.clientManager.Get(instance.ClientId)
			if client != nil {
				client.Write("joined")
			}
		} else {
			results := r.logs[logId].Execute(instance)
			if len(results) == 0 {
				continue
			}

			var batchResult string
			priorClientId := results[0].ClientId
			for _, res := range results {
				clientId := res.ClientId
				if clientId == priorClientId {
					if batchResult == "" {
						batchResult = res.Result
					} else {
						batchResult += " " + res.Result
					}
				} else {
					client := r.clientManager.Get(priorClientId)
					if client != nil {
						client.Write(batchResult)
					}
					batchResult = res.Result
					priorClientId = clientId
				}
			}
			client := r.clientManager.Get(priorClientId)
			if client != nil {
				client.Write(batchResult)
			}
		}
	}
}

func (r *Replicant) AcceptClient() {
	for {
		conn, err := r.acceptor.Accept()
		if err != nil {
			logger.Error(err)
			break
		}
		r.clientManager.Start(conn)
	}
}

func (r *Replicant) Monitor() {
	r.multipaxos.Monitor()
}

func (r *Replicant) TriggerElection() {
	r.multipaxos.TriggerElection()
}
