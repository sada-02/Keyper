package balancer

import (
	"context"

	"github.com/pysel/dkvs/prototypes"
	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	"github.com/pysel/dkvs/shared"
)

// RegisterPartition registers a partition in the balancer.
func (bs *BalancerServer) RegisterPartition(ctx context.Context, req *pbbalancer.RegisterPartitionRequest) (*pbbalancer.RegisterPartitionResponse, error) {
	err := bs.Balancer.RegisterPartition(ctx, req.Address)
	if err != nil {
		return nil, err
	}

	bs.eventHandler.Emit(&RegisterPartitionEvent{Address: req.Address})

	// partition successfully registered
	return &pbbalancer.RegisterPartitionResponse{}, nil
}

// GetId returns the next client id to be used by a client.
func (bs *BalancerServer) GetId(ctx context.Context, req *pbbalancer.GetIdRequest) (*pbbalancer.GetIdResponse, error) {
	return &pbbalancer.GetIdResponse{Id: bs.Balancer.NextClientId()}, nil
}

// ----- To be relayed requests -----

func (bs *BalancerServer) Get(ctx context.Context, req *prototypes.GetRequest) (res *prototypes.GetResponse, err error) {
	defer func() { bs.postCRUD(req.Id, err, req.String()) }()
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	// validate request structure
	if err = req.Validate(); err != nil && req.Id != 0 {
		return nil, err
	}

	// validate timestamp
	if err = bs.validateTs(req.String(), req.Id, req.Lamport); err != nil {
		return nil, err
	}

	response, err := bs.Balancer.Get(ctx, req.Key)
	if offlineErr, ok := err.(ErrPartitionsOffline); err != nil && !ok { // skip if no error or if error is not ErrPartitionsOffline
		return nil, err
	} else if ok {
		// if error is ErrPartitionsOffline, emit an event for each offline partition
		for i := 0; i < len(offlineErr.Addresses); i++ {
			bs.eventHandler.Emit(&PartitionOfflineEvent{Address: offlineErr.Addresses[i], Err: offlineErr.Errors[i]})
		}

		// TODO: figure out what to do with offline partitions
	}

	bs.eventHandler.Emit(&GetEvent{msg: req.String()})

	return response, nil
}

func (bs *BalancerServer) Set(ctx context.Context, req *prototypes.SetRequest) (res *prototypes.SetResponse, err error) {
	defer func() { bs.postCRUD(req.Id, err, req.String()) }()
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	// validate request structure
	if err = req.Validate(); err != nil && req.Id != 0 {
		return nil, err
	}

	// validate timestamp
	if err = bs.validateTs(req.String(), req.Id, req.Lamport); err != nil {
		return nil, err
	}

	range_, err := bs.getRangeFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	lamport := bs.Balancer.GetNextLamportForKey(req.Key)
	msg := shared.NewPrepareCommitMessage_Set(req.Key, req.Value, lamport)

	err = bs.AtomicMessage(ctx, range_, msg)
	if err != nil {
		return nil, err
	}

	bs.eventHandler.Emit(&SetEvent{msg: req.String()})

	return &prototypes.SetResponse{}, nil
}

func (bs *BalancerServer) Delete(ctx context.Context, req *prototypes.DeleteRequest) (res *prototypes.DeleteResponse, err error) {
	defer func() { bs.postCRUD(req.Id, err, req.String()) }()
	bs.mutex.Lock()
	defer bs.mutex.Unlock()

	// validate request structure
	if err = req.Validate(); err != nil && req.Id != 0 {
		return nil, err
	}

	// validate timestamp
	if err = bs.validateTs(req.String(), req.Id, req.Lamport); err != nil {
		return nil, err
	}

	range_, err := bs.getRangeFromKey(req.Key)
	if err != nil {
		return nil, err
	}

	lamport := bs.Balancer.GetNextLamportForKey(req.Key)
	msg := shared.NewPrepareCommitMessage_Delete(req.Key, lamport)

	err = bs.AtomicMessage(ctx, range_, msg)
	if err != nil {
		return nil, err
	}

	bs.eventHandler.Emit(&DeleteEvent{msg: req.String()})

	return &prototypes.DeleteResponse{}, nil
}

func (bs *BalancerServer) postCRUD(id uint64, err error, req string) {
	if err != nil {
		if eventError, ok := err.(shared.IsWarningEventError); ok {
			bs.eventHandler.Emit(eventError.WarningErrorToEvent(req))
		} else {
			bs.eventHandler.Emit(&shared.ErrorEvent{Req: req, Err: err})
		}
	}

	bs.Balancer.IncrementLamportForId(id)
}

func (bs *BalancerServer) validateTs(req string, id uint64, ts uint64) error {
	// validate timestamp
	if err := bs.Balancer.validateIdAgainstTimestamp(id, ts); err != nil {
		return ErrNotReadyForRequest{ClientId: id, CurrentClientTimestamp: bs.Balancer.GetLamportForId(id), ReceivedClientTimestamp: ts}
	}

	return nil
}
