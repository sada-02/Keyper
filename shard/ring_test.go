package shard

import (
	"testing"
)

func TestRingBasic(t *testing.T) {
	r := NewRing(50)
	r.AddNode("http://n1:8080")
	r.AddNode("http://n2:8080")
	r.AddNode("http://n3:8080")

	if len(r.Nodes()) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(r.Nodes()))
	}

	// keys should map to some node
	k1, ok := r.GetNode("apple")
	if !ok || k1 == "" {
		t.Fatalf("expected node for key apple")
	}
	k2, _ := r.GetNode("banana")
	k3, _ := r.GetNode("carrot")
	// small sanity: nodes may be same or different, but must be valid addresses we added
	valid := map[string]bool{
		"http://n1:8080": true,
		"http://n2:8080": true,
		"http://n3:8080": true,
	}
	if !valid[k1] || !valid[k2] || !valid[k3] {
		t.Fatalf("unexpected node assignments: %v, %v, %v", k1, k2, k3)
	}

	// Remove a node and ensure GetNode still returns some node (not panic)
	r.RemoveNode("http://n2:8080")
	_, ok = r.GetNode("apple")
	if !ok {
		t.Fatalf("expected node after removal")
	}
}
