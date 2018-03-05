package rpc

import (
	"net"
	"net/rpc"
)

func NewServer(name string, address Address, receiver interface{}) (*Server, error) {
	listener, err := net.Listen(address.Network, address.Address)
	if err != nil {
		return nil, err
	}
	server := rpc.NewServer()
	if err = server.RegisterName(name, receiver); err != nil {
		return nil, err
	}
	return &Server{address, server, listener}, nil
}

type Server struct {
	address  Address
	server   *rpc.Server
	listener net.Listener
}

func (s *Server) Run() {
	go s.server.Accept(s.listener)
}

func (s *Server) Address() Address {
	return s.address
}

func (s *Server) Stop() error {
	return s.listener.Close()
}
