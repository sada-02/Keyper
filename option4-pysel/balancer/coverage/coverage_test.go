package coverage_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/pysel/dkvs/balancer/coverage"
	pbbalancer "github.com/pysel/dkvs/prototypes/balancer"
	"github.com/pysel/dkvs/testutil"
	"github.com/stretchr/testify/require"
)

var (
	zeroInt          = new(big.Int).SetInt64(0)
	quarterInt       = new(big.Int).Div(testutil.HalfShaDomain, big.NewInt(2))
	halfInt          = testutil.HalfShaDomain
	threeQuartersInt = new(big.Int).Mul(quarterInt, big.NewInt(3))
	fullInt          = new(big.Int).Mul(testutil.HalfShaDomain, big.NewInt(2))
)

func TestGetTickByValue(t *testing.T) {
	defaultCoverage_ := defaulCoverage(t)

	tests := map[string]struct {
		value    *big.Int
		Coverage *coverage.Coverage
		expected *pbbalancer.Tick
	}{
		"Get tick at the beginning": {
			value:    zeroInt,
			Coverage: defaultCoverage_,
			expected: defaultCoverage_.Ticks[0],
		},
		"Get tick at the end": {
			value:    fullInt,
			Coverage: defaultCoverage_,
			expected: defaultCoverage_.Ticks[len(defaultCoverage_.Ticks)-1],
		},
		"Get second tick": {
			value:    quarterInt,
			Coverage: defaultCoverage_,
			expected: defaultCoverage_.Ticks[1],
		},
		"Get tick that doesn't exist": {
			value:    new(big.Int).SetInt64(-1),
			Coverage: defaultCoverage_,
			expected: nil,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			tick := test.Coverage.GetTickByValue(test.value)
			if test.expected != nil {
				tickDeepEqual(t, tick, test.expected)
			} else {
				require.Nil(t, test.expected)
			}
		})
	}
}

func TestAddTick(t *testing.T) {
	defaultCoverage_ := defaulCoverage(t)

	tests := map[string]struct {
		toAdd             *pbbalancer.Tick
		Coverage          *coverage.Coverage
		expectedTick      *pbbalancer.Tick
		expectedTickValue *big.Int
	}{
		"Add tick at the beginning": {
			toAdd:    coverage.NewTick(new(big.Int).SetInt64(1), 1),
			Coverage: &coverage.Coverage{nil},
			expectedTick: &pbbalancer.Tick{
				Covers: 1,
				Value:  new(big.Int).SetInt64(1).Bytes(),
			},
			expectedTickValue: new(big.Int).SetInt64(1),
		},
		"Add tick at the end": {
			toAdd:    coverage.NewTick(new(big.Int).Mul(fullInt, big.NewInt(2)), 0),
			Coverage: defaultCoverage_,
			expectedTick: &pbbalancer.Tick{
				Value: new(big.Int).Mul(fullInt, big.NewInt(2)).Bytes(),
			},
			expectedTickValue: new(big.Int).Mul(fullInt, big.NewInt(2)),
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			test.Coverage.AddTick(test.toAdd)
			tickDeepEqual(t, test.Coverage.GetTickByValue(test.expectedTickValue), test.expectedTick)
		})
	}
}

// defaulCoverage creates a Coverage with the following ticks:
// - tick at 0 [min]
// - tick at 1/4 of the domain [min and max]
// - tick at 1/2 of the domain [max]
// - tick at 3/4 of the domain [min and max]
// - tick at the end of the domain [max]
//
// Visually ("-" denotes areas that are covered):
//
// 0-----1/4-----1/2     3/4-----1
func defaulCoverage(t *testing.T) *coverage.Coverage {
	Coverage := &coverage.Coverage{nil}

	// zeroNull corresponds to tick at 0
	zeroNull := coverage.NewTick(zeroInt, 1)

	// tickQuarter corresponds to tick at 1/4 of the domain
	tickQuarter := coverage.NewTick(quarterInt, 1)

	// tickHalf corresponds to tick at 1/2 of the domain
	tickHalf := coverage.NewTick(halfInt, 0)

	// tickThreeQuarters corresponds to tick at 3/4 of the domain
	tickThreeQuarters := coverage.NewTick(threeQuartersInt, 1)

	// tickFull corresponds to tick at the end of the domain
	tickFull := coverage.NewTick(fullInt, 0)

	Coverage.AddTick(zeroNull)
	Coverage.AddTick(tickQuarter)
	Coverage.AddTick(tickHalf)
	Coverage.AddTick(tickThreeQuarters)
	Coverage.AddTick(tickFull)

	assertDefaultCoverage(t, Coverage)

	return Coverage
}

func assertDefaultCoverage(t *testing.T, c *coverage.Coverage) {
	require.Equal(t, 5, len(c.Ticks))
	one64 := int64(1)
	zero64 := int64(0)

	firstTick := c.Ticks[0]
	require.Equal(t, zeroInt.Bytes(), firstTick.Value)
	require.Equal(t, one64, firstTick.Covers)

	secondTick := c.Ticks[1]
	require.Equal(t, quarterInt.Bytes(), secondTick.Value)
	require.Equal(t, one64, secondTick.Covers)

	thirdTick := c.Ticks[2]
	require.Equal(t, halfInt.Bytes(), thirdTick.Value)
	require.Equal(t, zero64, thirdTick.Covers)

	fourthTick := c.Ticks[3]
	require.Equal(t, threeQuartersInt.Bytes(), fourthTick.Value)
	require.Equal(t, one64, fourthTick.Covers)

	fifthTick := c.Ticks[4]
	require.Equal(t, fullInt.Bytes(), fifthTick.Value)
	require.Equal(t, zero64, fifthTick.Covers)
}

func tickDeepEqual(t *testing.T, expected, actual *pbbalancer.Tick) {
	require.Equal(t, bytes.Compare(expected.Value, actual.Value), 0)
	require.Equal(t, expected.Covers, actual.Covers)
}
