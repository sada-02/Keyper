package partition

import (
	"log"

	pbpartition "github.com/pysel/dkvs/prototypes/partition"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewPartitionClient creates a new client to a partition.
func NewPartitionClient(addr string) pbpartition.PartitionServiceClient {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	client := pbpartition.NewPartitionServiceClient(conn)
	return client
}
