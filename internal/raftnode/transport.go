package raftnode

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// NewTCPTransport binds a raft.NetworkTransport on bindAddr, advertising
// advertiseAddr to peers. advertiseAddr must be reachable by other nodes
// (e.g. a Docker Compose service name, not "localhost"), or peers will be
// unable to dial this node once it is behind a container network.
func NewTCPTransport(bindAddr, advertiseAddr string, logOutput io.Writer) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("raftnode: resolve advertise addr %s: %w", advertiseAddr, err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, logOutput)
	if err != nil {
		return nil, fmt.Errorf("raftnode: new tcp transport on %s: %w", bindAddr, err)
	}
	return transport, nil
}
