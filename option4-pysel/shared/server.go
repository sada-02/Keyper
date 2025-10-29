package shared

import (
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
)

// startListeningOnPort starts a grpc server listening on the given port.
func StartListeningOnPort(s *grpc.Server, port uint64) (*sync.WaitGroup, net.Addr) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()

	return &wg, lis.Addr()
}
