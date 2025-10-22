package shard

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring implements a consistent hashing ring with virtual nodes.
// Each virtual node is represented by hash( nodeAddr + "#" + vnodeIndex )
type Ring struct {
	sync.RWMutex
	replicas int                 // virtual nodes per physical node
	keys     []uint32            // sorted hashes of virtual nodes
	vmap     map[uint32]string   // map hash -> node address (physical)
	nodes    map[string]struct{} // set of physical nodes
}

// NewRing creates a ring with given virtual node replicas (recommended 100-300).
func NewRing(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 100
	}
	return &Ring{
		replicas: replicas,
		vmap:     make(map[uint32]string),
		nodes:    make(map[string]struct{}),
	}
}

// hashKey returns a 32-bit hash for a string key.
func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode inserts a physical node (address string, e.g. "http://127.0.0.1:8080") into the ring.
func (r *Ring) AddNode(node string) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.nodes[node]; ok {
		return // already added
	}
	for i := 0; i < r.replicas; i++ {
		vkey := node + "#" + strconv.Itoa(i)
		h := hashKey(vkey)
		r.keys = append(r.keys, h)
		r.vmap[h] = node
	}
	r.nodes[node] = struct{}{}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// RemoveNode removes a physical node from the ring.
func (r *Ring) RemoveNode(node string) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	// remove virtual nodes
	toRemove := make(map[uint32]struct{})
	for i := 0; i < r.replicas; i++ {
		vkey := node + "#" + strconv.Itoa(i)
		h := hashKey(vkey)
		toRemove[h] = struct{}{}
		delete(r.vmap, h)
	}
	// rebuild keys slice without removed hashes
	newKeys := r.keys[:0]
	for _, k := range r.keys {
		if _, rem := toRemove[k]; !rem {
			newKeys = append(newKeys, k)
		}
	}
	r.keys = newKeys
	delete(r.nodes, node)
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

// GetNode returns the node address responsible for the given key.
func (r *Ring) GetNode(key string) (string, bool) {
	r.RLock()
	defer r.RUnlock()
	if len(r.keys) == 0 {
		return "", false
	}
	h := hashKey(key)
	// Binary search for first key >= h
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx == len(r.keys) {
		// wrap around to first
		idx = 0
	}
	node, ok := r.vmap[r.keys[idx]]
	return node, ok
}

// Nodes returns a copy of all physical nodes in the ring.
func (r *Ring) Nodes() []string {
	r.RLock()
	defer r.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
