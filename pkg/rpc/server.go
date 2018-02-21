package rpc

import (
	"net"
	"net/rpc"
)

func NewServer(name string, receiver interface{}, network, address string) (*Server, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	server := rpc.NewServer()
	if err = server.RegisterName(name, receiver); err != nil {
		return nil, err
	}
	return &Server{server, listener}, nil
}

type Server struct {
	server   *rpc.Server
	listener net.Listener
}

func (s *Server) Run() {
	go s.server.Accept(s.listener)
}

func (s *Server) Stop() error {
	return s.listener.Close()
}
