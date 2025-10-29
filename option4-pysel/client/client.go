package client

import (
	"context"

	"github.com/pysel/dkvs/balancer"
	"github.com/pysel/dkvs/partition"
	"github.com/pysel/dkvs/prototypes"
	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
)

// Client is an object that is responsible for interacting with DKVS.
type Client struct {
	// go context
	context context.Context

	// logical timestamp. Corresponds to the timestamp of the last PROCESSED message.
	timestamp uint64
	// client's id
	id uint64

	// a client to balancer server
	balacerClient pbbalancer.BalancerServiceClient

	// a list of messages that might have not yet processed by dkvs.
	// nonConfirmedList types.Backlog
}

// NewClient creates a new client instance
// During this process, the client gets an id and one the other side, the balancer registers this client
// * with lamport timestamp of 0.
func NewClient(context context.Context, balancerAddr string) *Client {
	c := &Client{
		context:       context,
		timestamp:     0,
		balacerClient: balancer.NewBalancerClient(balancerAddr),
	}

	id, err := c.balacerClient.GetId(context, &pbbalancer.GetIdRequest{})
	if err != nil {
		panic(err)
	}

	c.id = id.Id

	return c
}

// Set sets a value for a key.
func (c *Client) Set(key, value []byte) error {
	c.timestamp++

	req := &prototypes.SetRequest{
		Key:     key,
		Value:   value,
		Lamport: c.timestamp,
		Id:      c.id,
	}

	_, err := c.balacerClient.Set(c.context, req)
	switch err.(type) {
	case partition.ErrTimestampIsStale: // if timestamp is stale, it is a concurrency issue
		// TODO: retry previous requests
		return nil
	}

	return err
}

// Get gets a value for a key.
func (c *Client) Get(key []byte) ([]byte, error) {
	c.timestamp++

	req := &prototypes.GetRequest{
		Key:     key,
		Lamport: c.timestamp,
		Id:      c.id,
	}

	resp, err := c.balacerClient.Get(c.context, req)
	if err != nil {
		return nil, err
	}

	if resp.StoredValue == nil {
		return nil, nil
	}

	return resp.StoredValue.Value, nil
}

// Delete deletes a value for a key.
func (c *Client) Delete(key []byte) error {
	c.timestamp++

	req := &prototypes.DeleteRequest{
		Key:     key,
		Lamport: c.timestamp,
		Id:      c.id,
	}

	_, err := c.balacerClient.Delete(c.context, req)
	if err != nil {
		return err
	}

	return nil
}

// func (c *Client) processGrpcError(err error) {

// }
