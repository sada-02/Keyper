package balancer_test

import (
	"context"
	"os"
	"testing"

	"github.com/pysel/dkvs/balancer"
	"github.com/pysel/dkvs/partition"
	"github.com/pysel/dkvs/prototypes"
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
	"github.com/pysel/dkvs/testutil"
	"github.com/stretchr/testify/require"
)

func TestTwoPhaseCommit(t *testing.T) {
	defer os.RemoveAll(balancer.BalancerDBPath + t.Name())
	addrs, paths := testutil.StartXPartitionServers(t, 2)
	defer testutil.RemovePaths(paths)

	ctx := context.Background()

	partitionAddr1, partitionAddr2 := addrs[0], addrs[1]

	b := balancer.NewBalancer(balancer.BalancerDBPath+t.Name(), 1)

	err := b.RegisterPartition(ctx, partitionAddr1.String())
	require.NoError(t, err)

	err = b.RegisterPartition(ctx, partitionAddr2.String())
	require.NoError(t, err)

	range_, err := b.GetRangeFromKey(testutil.DomainKey)
	require.NoError(t, err)

	msgSet := &pbpartition.PrepareCommitRequest{
		Message: &pbpartition.PrepareCommitRequest_Set{
			Set: &prototypes.SetRequest{
				Key:     testutil.DomainKey,
				Value:   []byte("value"),
				Lamport: 1,
			},
		},
	}

	err = b.AtomicMessage(ctx, range_, msgSet)
	require.NoError(t, err)

	// Assert that value was stored correctly
	getResp, err := b.Get(ctx, testutil.DomainKey)
	require.NoError(t, err)

	expected := partition.ToStoredValue(1, []byte("value"))
	require.Equal(t, expected, getResp.StoredValue)

	msgDelete := &pbpartition.PrepareCommitRequest{
		Message: &pbpartition.PrepareCommitRequest_Delete{
			Delete: &prototypes.DeleteRequest{
				Key:     testutil.DomainKey,
				Lamport: 3,
			},
		},
	}

	err = b.AtomicMessage(ctx, range_, msgDelete)
	require.NoError(t, err)

	// Assert that value was deleted correctly
	getResp, err = b.Get(ctx, testutil.DomainKey)
	require.NoError(t, err)

	require.Nil(t, getResp.StoredValue)
}
