package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RequestJoin asks the cluster to add this node (nodeID, this node's own
// HTTP address, and one Raft advertise address per shard) as a voter in
// every shard group. It starts at seedHTTPAddr and, if that node reports it
// isn't the leader, follows the leader hint until the join succeeds or
// overallTimeout elapses -- the seed address does not need to actually be
// the leader. On success it returns the full node-ID -> HTTP-address map
// the leader knows about, so the caller can populate its own AddrBook and
// resolve leader hints for shards this node isn't the leader of.
func RequestJoin(seedHTTPAddr, nodeID, httpAddr string, raftAddrs []string, overallTimeout time.Duration) (map[string]string, error) {
	deadline := time.Now().Add(overallTimeout)
	addr := seedHTTPAddr
	var lastErr error

	for time.Now().Before(deadline) {
		peers, next, err := attemptJoin(addr, nodeID, httpAddr, raftAddrs, 5*time.Second)
		if err == nil {
			return peers, nil
		}
		lastErr = err
		if next == "" {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		addr = next
	}
	return nil, fmt.Errorf("api: join did not succeed within %s: %w", overallTimeout, lastErr)
}

// attemptJoin makes one join request against addr. If the response names a
// different leader, it returns that leader's address as leaderHint so the
// caller can retry there.
func attemptJoin(addr, nodeID, httpAddr string, raftAddrs []string, timeout time.Duration) (peers map[string]string, leaderHint string, err error) {
	body, err := json.Marshal(joinRequest{NodeID: nodeID, HTTPAddr: httpAddr, RaftAddrs: raftAddrs})
	if err != nil {
		return nil, "", fmt.Errorf("api: encode join request: %w", err)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(strings.TrimRight(addr, "/")+"/admin/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, "", fmt.Errorf("api: join request to %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var joinResp joinResponse
		if err := json.NewDecoder(resp.Body).Decode(&joinResp); err != nil {
			return nil, "", fmt.Errorf("api: decode join response from %s: %w", addr, err)
		}
		return joinResp.Peers, "", nil
	}

	var errResp errorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp.Leader != "" {
		return nil, errResp.Leader, fmt.Errorf("api: %s is not the leader", addr)
	}
	return nil, "", fmt.Errorf("api: join request to %s failed: %s (status %d)", addr, errResp.Error, resp.StatusCode)
}
