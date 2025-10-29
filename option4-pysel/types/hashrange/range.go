package hashrange

import (
	"math/big"
	"strings"

	"github.com/pysel/dkvs/types"
)

// A range of keys this partition is responsible for. Total range is [0; 2^256].
type Range struct {
	Min *big.Int
	Max *big.Int
}

var (
	MinInt *big.Int
	MaxInt *big.Int
)

func init() {
	MinInt = new(big.Int).SetInt64(0)
	MaxInt_bz := make([]byte, 32)
	for i := 0; i < 32; i++ {
		MaxInt_bz[i] = 0xFF // a byte with all bits set to 1
	}

	MaxInt = new(big.Int).SetBytes(MaxInt_bz)
}

// NewRange is a constructor for Range.
func NewRange(minb, maxb []byte) *Range {
	min := new(big.Int).SetBytes(minb)
	max := new(big.Int).SetBytes(maxb)

	if min.Cmp(MinInt) == -1 {
		// min should be >= 0, since SHA-2 only produces positive hashes.
		panic("min is negative")
	}

	if max.Cmp(MaxInt) == 1 {
		// max should be lower than maximum possible hash.
		panic("max is greater than 2^256")
	}

	if max.Cmp(min) == 0 {
		// min and max should be different.
		panic("min and max are equal")
	}

	if max.Cmp(min) == -1 {
		// max should be greater than min.
		panic("max is less than min")
	}

	return &Range{min, max}
}

// Contains checks if the given key is in the range.
func (r *Range) Contains(hash []byte) bool {
	hashInt := new(big.Int).SetBytes(hash[:])
	return r.Min.Cmp(hashInt) <= 0 && r.Max.Cmp(hashInt) >= 0
}

type RangeKey string

func (r RangeKey) ToRange() (*Range, error) {
	splitted := strings.Split(string(r), "; ")
	min, ok := new(big.Int).SetString(splitted[0], 10)
	if !ok {
		return nil, types.ErrFailedToSetString

	}

	max, err := new(big.Int).SetString(splitted[1], 10)
	if !err {
		return nil, types.ErrFailedToSetString
	}
	return NewRange(min.Bytes(), max.Bytes()), nil
}

func (r *Range) AsKey() RangeKey {
	return RangeKey(r.Min.String() + "; " + r.Max.String())
}
