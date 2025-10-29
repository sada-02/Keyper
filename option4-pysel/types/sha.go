package types

import "crypto/sha256"

func ShaKey(key []byte) [32]byte {
	checksum := sha256.Sum256(key)
	return checksum
}
