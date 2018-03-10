package worker

import (
	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/types"
)

type Server struct {
	worker *Worker
	server *rpc.Server
}

func newRpcServer(worker *Worker, address rpc.Address) (*Server, error) {
	server := &Server{worker: worker}
	rpcServer, err := rpc.NewServer(worker.name, address, server)
	if err != nil {
		return nil, err
	}
	server.server = rpcServer
	return server, nil
}

func (s *Server) run() {
	s.server.Run()
}

func (s *Server) address() rpc.Address {
	return s.server.Address()
}

func (s *Server) Execute(task *types.Task, result *types.Result) error {
	return s.worker.execute(task, result)
}

func (s *Server) Shutdown(_ rpc.NopValue, _ *rpc.NopValue) error {
	s.worker.shutdown()
	return s.server.Stop()
}
