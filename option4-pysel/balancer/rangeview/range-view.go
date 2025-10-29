package rangeview

import (
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
)

// RangeView is an abstraction over clients to partitions responsible for the same range
type RangeView struct {
	Clients   []*pbpartition.PartitionServiceClient
	Addresses []string
	Lamport   uint64
}

func NewRangeView(clients []*pbpartition.PartitionServiceClient, addresses []string) *RangeView {
	return &RangeView{Clients: clients, Addresses: addresses, Lamport: 0}
}

// AddPartitionClient adds a client to the set of clients in a range view
func (rv *RangeView) AddPartitionData(client *pbpartition.PartitionServiceClient, address string) {
	rv.Clients = append(rv.Clients, client)
	rv.Addresses = append(rv.Addresses, address)
}

func (rv *RangeView) GetResponsibleClients() []*pbpartition.PartitionServiceClient {
	return rv.Clients
}

// RemovePartition removes a partition from the balancer's registry.
func (rv *RangeView) RemovePartition(addr string) error {
	for i, address := range rv.Addresses {
		if address == addr {
			rv.Clients = append(rv.Clients[:i], rv.Clients[i+1:]...)
			rv.Addresses = append(rv.Addresses[:i], rv.Addresses[i+1:]...)
			return nil
		}
	}
	return ErrPartitionAtAddressNotExist
}
