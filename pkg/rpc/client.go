package rpc

import (
	"net"
	"net/rpc"
)

func NewClient(network, address string) (*Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return &Client{rpc.NewClient(conn)}, nil
}

type Client struct {
	client *rpc.Client
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return c.client.Call(serviceMethod, args, reply)
}

func (c *Client) Close() error {
	return c.client.Close()
}
