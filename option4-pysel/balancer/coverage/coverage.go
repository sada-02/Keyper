package coverage

import (
	"bytes"
	"fmt"
	"math/big"

	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	hashrange "github.com/pysel/dkvs/types/hashrange"
)

var CreatedCoverage *Coverage

func NewTick(value *big.Int, covers int64) *pbbalancer.Tick {
	return &pbbalancer.Tick{
		Covers: covers,
		Value:  value.Bytes(),
	}
}

// Coverage is a linked list of initialized ticks.
type Coverage struct{ Ticks []*pbbalancer.Tick }

func (c *Coverage) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("Coverage:")
	for _, tick := range c.Ticks {
		buffer.WriteString("\n | Tick: ")
		buffer.WriteString("\n | | Value: " + new(big.Int).SetBytes(tick.Value).String())
		buffer.WriteString("\n | | Covers: " + fmt.Sprint(tick.Covers))
		buffer.WriteString("\n |\n |")
	}
	return buffer.String()
}

// GetCoverage returns a Coverage.
// Singletone pattern is used here.
// func GetCoverage() *Coverage {
// 	if CreatedCoverage == nil {
// 		CreatedCoverage = &Coverage{nil}
// 	}
// 	return &Coverage{nil}
// }

// addTick iterates over the list of ticks until
func (c *Coverage) AddTick(t *pbbalancer.Tick) {
	if len(c.Ticks) == 0 {
		c.Ticks = append(c.Ticks, t)
		return
	}

	// find the tick that is greater than the new tick
	ind := 0
	for ; ind < len(c.Ticks); ind++ {
		if bytes.Compare(c.Ticks[ind].Value, t.Value) > 0 {
			break
		}
	}

	// if the tick is not found, append it to the end
	if ind == len(c.Ticks) {
		c.Ticks = append(c.Ticks, t)
	} else {
		// if the tick is found, insert it
		c.Ticks = append(append(c.Ticks[:ind+1], t), c.Ticks[ind+1:]...)
	}
}

// GetNextPartitionRange is used when assigning a range to a newly registered partition
func (c *Coverage) GetNextPartitionRange() (hashrange.RangeKey, *pbbalancer.Tick, *pbbalancer.Tick) {
	// initially assume that first interval is minimal
	minCovered := c.Ticks[0].Covers
	minLowerTick := c.Ticks[0]
	minUpperTick := c.Ticks[1]
	minRange := hashrange.NewRange(minLowerTick.Value, minUpperTick.Value)
	for ind, tick := range c.Ticks[:len(c.Ticks)-1] { // no need to cover last
		nextTick := c.Ticks[ind+1]
		if tick.Covers < minCovered {
			minRange = hashrange.NewRange(tick.Value, nextTick.Value)
			minCovered = tick.Covers
			minLowerTick = tick
			minUpperTick = nextTick
		}
	}

	// minLowerTick and minUpperTick are returned to be increased by 1 if a partition is successfully registered
	return hashrange.RangeKey(minRange.AsKey()), minLowerTick, minUpperTick
}

func (c *Coverage) BumpTicks(lowerTick *pbbalancer.Tick) {
	lowerTick.Covers++
}

// ToProto converts Coverage to protobuf Coverage
func (c *Coverage) ToProto() *pbbalancer.Coverage {
	return &pbbalancer.Coverage{
		Ticks: c.Ticks,
	}
}

func (c *Coverage) GetTickByValue(value *big.Int) *pbbalancer.Tick {
	for _, tick := range c.Ticks {
		if bytes.Equal(tick.Value, value.Bytes()) {
			return tick
		}
	}

	return nil
}
