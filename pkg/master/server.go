package master

import (
	"github.com/mlmhl/mapreduce/pkg/rpc"
	"github.com/mlmhl/mapreduce/pkg/types"
)

type Server struct {
	master *Master
	server *rpc.Server
}

func newServer(master *Master, address rpc.Address) (*Server, error) {
	server := &Server{master: master}
	rpcServer, err := rpc.NewServer(master.job.Name, address, server)
	if err != nil {
		return nil, err
	}
	server.server = rpcServer
	return server, nil
}

func (s *Server) run() {
	s.server.Run()
}

func (s *Server) stop() {
	s.server.Stop()
}

func (s *Server) Register(arg types.RegisterArg, result *types.RegisterResult) error {
	err := s.master.addWorker(arg.Name, arg.Balance, arg.Address)
	if err == nil {
		result.Result = types.SucceededResult
	} else {
		result.Result.Code = types.RpcCallErr
		result.Result.Message = err.Error()
	}
	return nil
}

// TODO: Worker maybe already removed as network partition,
// add the worker again if the worker not exist.
func (s *Server) Ping(arg types.PingArg, result *types.Result) error {
	s.master.workerHeartBeat(arg.Name)
	*result = types.SucceededResult
	return nil
}
