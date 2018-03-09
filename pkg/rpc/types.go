package rpc

import (
	"fmt"
	"strings"
)

var validNetwork = map[string]bool{
	"udp": true,
	"tcp": true,
}

type Address struct {
	Network string
	Address string
}

func ParseAddress(str string) (Address, error) {
	tags := strings.SplitN(str, ":", 2)
	if len(tags) != 2 {
		return Address{}, fmt.Errorf("unknown address format: %s", str)
	}
	if !validNetwork[tags[0]] {
		return Address{}, fmt.Errorf("unknown network: %s", tags[0])
	}
	return Address{Network: tags[0], Address: tags[1]}, nil
}

func (a Address) String() string {
	return fmt.Sprintf("%s/%s", a.Network, a.Address)
}

type NopValue struct{}
