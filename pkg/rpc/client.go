package rpc

import (
	"net"
	"net/rpc"
)

func NewClient(address Address) (*Client, error) {
	conn, err := net.Dial(address.Network, address.Address)
	if err != nil {
		return nil, err
	}
	return &Client{client: rpc.NewClient(conn), address: address}, nil
}

type Client struct {
	client  *rpc.Client
	address Address
}

func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	return c.client.Call(serviceMethod, args, reply)
}

func (c *Client) Address() Address {
	return c.address
}

func (c *Client) Close() error {
	return c.client.Close()
}
