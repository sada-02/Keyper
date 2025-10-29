package partition

import (
	"bytes"
)

// checkKeyRange checks if the key is in the partition's range.
func (p *Partition) checkKeyRange(key []byte) error {
	if len(key) != 32 {
		return ErrInvalidKeySize
	}
	if bytes.Compare(key, p.hashrange.Min.Bytes()) == -1 || bytes.Compare(key, p.hashrange.Max.Bytes()) == 1 {
		// Key is out of range.
		return ErrNotThisPartitionKey
	}

	return nil
}
