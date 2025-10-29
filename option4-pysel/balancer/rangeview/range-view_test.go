package rangeview_test

import (
	"context"
	"testing"

	"github.com/pysel/dkvs/balancer/rangeview"
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
	"github.com/pysel/dkvs/testutil"
	"github.com/stretchr/testify/require"
)

func TestRemovePartition(t *testing.T) {
	defer testutil.RemovePaths([]string{testutil.TestDBPath})

	ctx := context.Background()

	addr, client, closer := testutil.StartPartitionClientToBufferedServer(ctx)
	defer closer()

	rv := rangeview.NewRangeView([]*pbpartition.PartitionServiceClient{&client}, []string{addr.String()})

	err := rv.RemovePartition("invalid address")
	require.Equal(t, rangeview.ErrPartitionAtAddressNotExist, err)

	require.Equal(t, 1, len(rv.GetResponsibleClients()))
	require.Equal(t, 1, len(rv.GetAddresses()))

	err = rv.RemovePartition(addr.String())
	require.NoError(t, err)

	require.Equal(t, 0, len(rv.GetResponsibleClients()))
	require.Equal(t, 0, len(rv.GetAddresses()))
}
