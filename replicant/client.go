package replicant

import (
	"bufio"
	"net"
	"strconv"
	"strings"

	"github.com/psu-csl/replicated-store/go/multipaxos"
	pb "github.com/psu-csl/replicated-store/go/multipaxos/comm"
)

type Client struct {
	id         int64
	reader     *bufio.Reader
	writer     *bufio.Writer
	socket     net.Conn
	multipaxos *multipaxos.Multipaxos
	manager    *ClientManager
}

func NewClient(id int64, conn net.Conn, mp *multipaxos.Multipaxos,
	manger *ClientManager) *Client {
	client := &Client{
		id:         id,
		reader:     bufio.NewReader(conn),
		writer:     bufio.NewWriter(conn),
		socket:     conn,
		multipaxos: mp,
		manager:    manger,
	}
	return client
}

func (c *Client) ParseCommand(substrings []string) *pb.Command {
	if len(substrings) < 2 {
		return nil
	}
	commandType := substrings[0]
	key := substrings[1]

	command := &pb.Command{Key: key}

	if commandType == "get" {
		command.Type = pb.CommandType_GET
	} else if commandType == "del" {
		command.Type = pb.CommandType_DEL
	} else if commandType == "put" {
		if len(substrings) != 3 {
			return nil
		}
		command.Type = pb.CommandType_PUT
		command.Value = substrings[2]
	} else if commandType == "add" {
		if len(substrings) != 3 {
			return nil
		}
		command.Type = pb.CommandType_ADDNODE
		command.Value = substrings[2]
	} else {
		return nil
	}
	return command
}

func (c *Client) Parse(request string) []*pb.Command {
	substrings := strings.SplitN(strings.TrimRight(request, "\n"), " ", 4)
	if len(substrings) < 3 {
		return nil
	}

	numCommands, err := strconv.Atoi(substrings[0])
	if err != nil || numCommands <= 0 {
		return nil
	}

	command := c.ParseCommand(substrings[1:])
	if command == nil {
		return nil
	}

	commands := make([]*pb.Command, 0, numCommands)
	for i := 0; i < numCommands; i++ {
		command.ClientId = []int64{c.id}
		commands = append(commands, command)
	}
	return commands
}

func (c *Client) Start() {
	go c.Read()
}

func (c *Client) Stop() {
	c.socket.Close()
}

func (c *Client) Read() {
	for {
		request, err := c.reader.ReadString('\n')
		if err != nil {
			c.manager.Stop(c.id)
			return
		}

		commands := c.Parse(request)
		if len(commands) > 0 {
			resultChan := c.multipaxos.Replicate(commands, c.id)
			result := <-resultChan
			if result.Type == multipaxos.Ok {
				if result.Value == "" {
					continue
				}
				c.Write(result.Value)
			} else if result.Type == multipaxos.Retry {
				c.Write("retry")
			} else if result.Type == multipaxos.SomeElseLeader {
				c.Write("leader is " + strconv.FormatInt(result.Leader, 10))
			} else {
				panic("Result is not someone_else_leader")
			}
		} else {
			c.Write("bad command")
		}
	}
}

func (c *Client) Write(response string) {
	_, err := c.writer.WriteString(response + "\n")
	if err == nil {
		c.writer.Flush()
	}
}
