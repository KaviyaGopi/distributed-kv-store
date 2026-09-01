package api

import "sync"

// MemAddrBook is a concurrency-safe, in-memory AddrBook.
type MemAddrBook struct {
	mu    sync.RWMutex
	addrs map[string]string
}

// NewMemAddrBook returns an empty MemAddrBook.
func NewMemAddrBook() *MemAddrBook {
	return &MemAddrBook{addrs: make(map[string]string)}
}

// Set records the HTTP address for nodeID.
func (b *MemAddrBook) Set(nodeID, httpAddr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.addrs[nodeID] = httpAddr
}

// HTTPAddrFor implements AddrBook.
func (b *MemAddrBook) HTTPAddrFor(nodeID string) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	addr, ok := b.addrs[nodeID]
	return addr, ok
}

// All returns a snapshot of every known nodeID -> HTTP address mapping.
func (b *MemAddrBook) All() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.addrs))
	for k, v := range b.addrs {
		out[k] = v
	}
	return out
}
