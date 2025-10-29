package types_test

import (
	"testing"

	"github.com/pysel/dkvs/prototypes"
	"github.com/pysel/dkvs/types"
	"google.golang.org/protobuf/proto"
)

func TestBacklog(t *testing.T) {
	messages := []proto.Message{
		&prototypes.SetRequest{
			Key:     []byte("key"),
			Value:   []byte("value"),
			Lamport: 0,
			Id:      1,
		},
		&prototypes.SetRequest{
			Key:     []byte("key"),
			Value:   []byte("value"),
			Lamport: 1,
			Id:      1,
		},
		&prototypes.DeleteRequest{
			Key:     []byte("key"),
			Lamport: 2,
			Id:      1,
		},
		&prototypes.GetRequest{
			Key:     []byte("key"),
			Lamport: 3,
			Id:      1,
		},
	}

	b := types.NewBacklog()
	for i, msg := range messages {
		b.Add(uint64(i), msg)
	}

	for i, msg := range messages {
		lamport, popped := b.Pop()
		if !proto.Equal(msg, popped) || lamport != uint64(i) {
			t.Errorf("expected %v, got %v", msg, popped)
		}
	}

}
