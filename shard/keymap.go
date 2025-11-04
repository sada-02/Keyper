package shard

import (
	"hash/crc32"
	"strconv"
)

// KeyToShard maps a key to a shard id string in range [0, shardCount-1].
// Returns empty string if shardCount <= 0.
func KeyToShard(key string, shardCount int) string {
	if shardCount <= 0 {
		return ""
	}
	h := crc32.ChecksumIEEE([]byte(key))
	idx := int(h % uint32(shardCount))
	return strconv.Itoa(idx)
}
