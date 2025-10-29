package balancer

import (
	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	"github.com/pysel/dkvs/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type BalancerServer struct {
	pbbalancer.UnimplementedBalancerServiceServer

	eventHandler *shared.EventHandler
	*Balancer
}

// RegisterBalancerServer creates a new grpc server and registers the balancer service.
func RegisterBalancerServer(b *Balancer) *grpc.Server {
	s := grpc.NewServer()
	eh := shared.NewEventHandler()

	reflection.Register(s)
	pbbalancer.RegisterBalancerServiceServer(s, &BalancerServer{Balancer: b, eventHandler: eh})

	return s
}
