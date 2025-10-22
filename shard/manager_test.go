package shard

import "testing"

func TestShardManagerBasic(t *testing.T) {
	m := NewManager()
	if m.HasShard("0") {
		t.Fatalf("expected no shard initially")
	}
	m.AddShard("0")
	if !m.HasShard("0") {
		t.Fatalf("expected shard 0 after add")
	}
	list := m.List()
	if len(list) != 1 {
		t.Fatalf("expected list length 1 got %d", len(list))
	}
	m.RemoveShard("0")
	if m.HasShard("0") {
		t.Fatalf("expected shard 0 removed")
	}
}