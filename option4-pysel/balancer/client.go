package balancer

import (
	"log"
	"sync"

	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewBalancerClient creates a new client to a balancer.
func NewBalancerClient(addr string) pbbalancer.BalancerServiceClient {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	client := pbbalancer.NewBalancerServiceClient(conn)
	return client
}

// clientIdToLamport is used to map ids of clients to their processed logical timestamps.
type clientIdToLamport struct {
	map_  map[uint64]uint64
	mutex *sync.Mutex
}

func NewClientIdToLamport() *clientIdToLamport {
	return &clientIdToLamport{
		map_:  make(map[uint64]uint64),
		mutex: new(sync.Mutex),
	}
}
