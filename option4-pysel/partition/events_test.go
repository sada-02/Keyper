package partition_test

import (
	"os"
	"testing"

	"github.com/pysel/dkvs/partition"
	"github.com/pysel/dkvs/testutil"
	"github.com/stretchr/testify/require"
)

func TestPartitionEvents(t *testing.T) {
	defer os.RemoveAll(testutil.TestDBPath)

	p := partition.NewPartition(testutil.TestDBPath)
	defer p.Close()

	p.SetHashrange(testutil.DefaultHashrange)

	// Set
	err := p.Set(testutil.DomainKey, []byte("Value"))
	require.NoError(t, err)
}
