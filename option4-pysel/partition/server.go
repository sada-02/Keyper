package partition

import (
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ListenServer struct {
	pbpartition.UnimplementedPartitionServiceServer

	*Partition
}

func RegisterPartitionServer(partition *Partition) *grpc.Server {
	s := grpc.NewServer()
	reflection.Register(s)
	pbpartition.RegisterPartitionServiceServer(s, &ListenServer{Partition: partition})

	return s
}
