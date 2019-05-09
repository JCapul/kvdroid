package kvdroid

import (
	"fmt"
)

// Ring ...
type Ring struct {
	clients map[string]*Client
	hash    *ConsistentHash
}

// NewRing ...
func NewRing(addrs []string) *Ring {

	ids := make([]string, len(addrs))
	clients := make(map[string]*Client)
	for i, addr := range addrs {
		ids[i] = fmt.Sprintf("%d", i)
		clients[ids[i]] = NewClient(addr)
	}
	hash := NewConsistentHash(100, nil)
	hash.Add(ids...)

	return &Ring{
		clients: clients,
		hash:    hash,
	}
}

// GetClient ...
func (r *Ring) GetClient(key string) *Client {
	return r.clients[r.hash.Get(key)]
}

// Close ...
func (r *Ring) Close() error {
	var err error
	for _, client := range r.clients {
		err = client.Close()
	}
	return err
}

// GetBytes ...
func (r *Ring) GetBytes(key string) ([]byte, error) {
	return r.GetClient(key).GetBytes(key)
}

// GetBytesUinto ...
func (r *Ring) GetBytesUinto(key string, dst []byte) (uint32, error) {
	return r.GetClient(key).GetBytesInto(key, dst)
}

// GetBytesRange ...
func (r *Ring) GetBytesRange(key string, start, end uint32) ([]byte, error) {
	return r.GetClient(key).GetBytesRange(key, start, end)
}

// GetBytesRangeUinto ...
func (r *Ring) GetBytesRangeUinto(key string, start, end uint32, dst []byte) (uint32, error) {
	return r.GetClient(key).GetBytesRangeInto(key, start, end, dst)
}

// SetBytes ...
func (r *Ring) SetBytes(key string, data []byte) {
	r.GetClient(key).SetBytes(key, data)
}

// SetBytesRange ...
func (r *Ring) SetBytesRange(key string, start uint32, data []byte) {
	r.GetClient(key).SetBytesRange(key, start, data)
}

// DelBytes ...
func (r *Ring) DelBytes(key string) error {
	return r.GetClient(key).DelBytes(key)
}

// TruncateBytes ...
func (r *Ring) TruncateBytes(key string, size uint32) error {
	return r.GetClient(key).TruncateBytes(key, size)
}

// SetUint ...
func (r *Ring) SetUint(key string, val uint32) {
	r.GetClient(key).SetUint(key, val)
}

// GetUint ...
func (r *Ring) GetUint(key string) (uint32, error) {
	return r.GetClient(key).GetUint(key)
}

// DelUint ...
func (r *Ring) DelUint(key string) error {
	return r.GetClient(key).DelUint(key)
}

// SetUintIfMax ...
func (r *Ring) SetUintIfMax(key string, val uint32) {
	r.GetClient(key).SetUintIfMax(key, val)
}
