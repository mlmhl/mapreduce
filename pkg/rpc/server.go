package rpc

import (
	"net"
	"net/rpc"

	"github.com/golang/glog"
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
	return &Server{address, server, listener, make(chan struct{})}, nil
}

type Server struct {
	address  Address
	server   *rpc.Server
	listener net.Listener
	shutdown chan struct{}
}

func (s *Server) Run() {
	go func() {
		for {
			if s.stopped() {
				break
			}
			conn, err := s.listener.Accept()
			if err != nil {
				glog.Errorf("RPC server: accept error: %v", err)
				break
			}
			go func() {
				defer conn.Close()
				s.server.ServeConn(conn)
			}()
		}
		if err := s.listener.Close(); err != nil {
			glog.Errorf("Listener close error: %v", err)
		} else {
			glog.V(4).Infof("RPC server stopped")
		}
	}()
}

func (s *Server) Address() Address {
	return s.address
}

func (s *Server) Stop() {
	close(s.shutdown)
}

func (s *Server) stopped() bool {
	select {
	case <-s.shutdown:
		return true
	default:
	}
	return false
}
