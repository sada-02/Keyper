package balancer

import (
	"math/big"

	"github.com/pysel/dkvs/balancer/rangeview"
	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	"github.com/pysel/dkvs/types/hashrange"
)

type (
	ClientIdToLamport clientIdToLamport
)

func (b *Balancer) GetTickByValue(value *big.Int) *pbbalancer.Tick {
	return b.coverage.GetTickByValue(value)
}

func (b *Balancer) GetCoverageSize() int {
	return len(b.coverage.Ticks)
}

func (b *Balancer) GetNextPartitionRange() (hashrange.RangeKey, *pbbalancer.Tick, *pbbalancer.Tick) {
	return b.coverage.GetNextPartitionRange()
}

func (b *Balancer) GetRangeFromKey(key []byte) (*hashrange.Range, error) {
	return b.getRangeFromKey(key)
}

func (b *Balancer) GetRangeToViews() map[hashrange.RangeKey]*rangeview.RangeView {
	return b.rangeToViews
}
