package partition_test

import (
	"math/big"
	"os"
	"testing"

	"github.com/pysel/dkvs/partition"
	"github.com/pysel/dkvs/testutil"
	hashrange "github.com/pysel/dkvs/types/hashrange"
	"github.com/stretchr/testify/require"
)

// Half of MaxInt
var defaultHashrange = hashrange.NewRange(big.NewInt(0).Bytes(), new(big.Int).Div(hashrange.MaxInt, big.NewInt(2)).Bytes())

func TestDatabaseMethods(t *testing.T) {
	p := partition.NewPartition("test")
	p.SetHashrange(defaultHashrange)

	defer p.Close()
	defer require.NoError(t, os.RemoveAll("test"))

	err := p.Set(testutil.DomainKey, []byte("Value"))
	require.NoError(t, err)

	err = p.Set(testutil.NonDomainKey, []byte("Value2"))
	require.Error(t, err) // not partition's key, should return error

	value, err := p.Get(testutil.DomainKey)
	require.NoError(t, err) // partition's key, should get correctly
	require.Equal(t, []byte("Value"), value)

	value, err = p.Get(testutil.NonDomainKey)
	require.Error(t, err) // not partition's key, should return error
	require.Nil(t, value)

	err = p.Delete(testutil.NonDomainKey)
	require.Error(t, err) // not partition's key, should return error

	err = p.Delete(testutil.DomainKey)
	require.NoError(t, err) // partition's key, should delete correctly

	value, err = p.Get(testutil.DomainKey)
	require.NoError(t, err) // partition's key, should return error
	require.Nil(t, value)
}
