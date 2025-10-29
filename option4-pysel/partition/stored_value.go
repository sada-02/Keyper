package partition

import (
	"github.com/pysel/dkvs/prototypes"
	"google.golang.org/protobuf/proto"
)

// toStoredValue converts a value with lamport timestamp to a stored value.
func ToStoredValue(lamport uint64, value []byte) *prototypes.StoredValue {
	return &prototypes.StoredValue{
		Lamport: lamport,
		Value:   value,
	}
}

type Request interface {
	GetValue() []byte
	GetLamport() uint64
}

// reqToBytes converts a request to marshalled bytes. It expects to get a struct that has a Value and Lamport field.
func reqToBytes(req Request) ([]byte, error) {
	storedValue := ToStoredValue(req.GetLamport(), req.GetValue())
	marshalled, err := proto.Marshal(storedValue)
	if err != nil {
		return nil, err
	}

	return marshalled, nil
}
