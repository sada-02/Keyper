package client

import (
	"context"

	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
)

func (c *Client) GetTimestamp() uint64 {
	return c.timestamp
}

func (c *Client) GetId() uint64 {
	return c.id
}

func (c *Client) GetBalancerClient() *pbbalancer.BalancerServiceClient {
	return &c.balacerClient
}

func (c *Client) GetContext() *context.Context {
	return &c.context
}
