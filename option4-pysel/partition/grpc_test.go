package partition_test

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/pysel/dkvs/partition"
	"github.com/pysel/dkvs/prototypes"
	"github.com/pysel/dkvs/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestGRPCServer(t *testing.T) {
	ctx := context.Background()
	_, client, closer := testutil.StartPartitionClientToBufferedServer(ctx)
	require.NotNil(t, closer)

	min := binary.LittleEndian.AppendUint64(testutil.DefaultHashrange.Min.Bytes(), 0)
	max := testutil.DefaultHashrange.Max.Bytes()
	_, err := client.SetHashrange(ctx, &prototypes.SetHashrangeRequest{
		Min: min,
		Max: max,
	})

	require.NoError(t, err)

	defer closer()
	defer require.NoError(t, os.RemoveAll(testutil.TestDBPath))

	tests := map[string]struct {
		request         proto.Message
		key             []byte
		increaseLamport bool

		expectedResponse string // we cannot directly compare proto.Message instances, hence, we compare string versions
		expectedError    error
	}{
		"Valid Set Request": {
			request:          &prototypes.SetRequest{},
			key:              testutil.DomainKey,
			expectedResponse: (&prototypes.SetResponse{}).String(),
			increaseLamport:  true,
			expectedError:    nil,
		},
		"Valid Delete Request": {
			request:          &prototypes.DeleteRequest{},
			key:              testutil.DomainKey,
			expectedResponse: (&prototypes.DeleteResponse{}).String(),
			increaseLamport:  true,
			expectedError:    nil,
		},
		"Invalid Set Request: nil key": {
			request:          &prototypes.SetRequest{},
			key:              nil,
			expectedResponse: "",
			increaseLamport:  false,
			expectedError:    errors.New("value length must be at least 1 bytes"),
		},
		"Invalid Set Request: key out of hashrange": {
			request:          &prototypes.SetRequest{},
			key:              testutil.NonDomainKey,
			expectedResponse: "",
			increaseLamport:  false,
			expectedError:    partition.ErrNotThisPartitionKey,
		},
	}

	// value and lamport are new for every test to avoid conflicting assertions between tests
	lamport := 1
	for _, test := range tests {
		test := test
		value := append([]byte("value"), byte(lamport))

		// send request logic: assert no errors and correct responses
		switch test.request.(type) {
		case *prototypes.SetRequest:
			req := test.request.(*prototypes.SetRequest)
			req.Lamport = uint64(lamport)
			req.Value = value
			req.Key = test.key

			resp, err := client.Set(ctx, req)
			if test.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError.Error())
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedResponse, resp.String())
			}

		case *prototypes.DeleteRequest:
			// prior to deleting, there should be a value stored
			setReq := &prototypes.SetRequest{
				Key:     test.key,
				Value:   value,
				Lamport: uint64(lamport),
			}
			_, err := client.Set(ctx, setReq)
			require.NoError(t, err)

			lamport++
			time.Sleep(50 * time.Millisecond)

			req := test.request.(*prototypes.DeleteRequest)
			req.Key = test.key
			req.Lamport = uint64(lamport)

			resp, err := client.Delete(ctx, req)
			if test.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, test.expectedError.Error())
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedResponse, resp.String())
			}
		}

		if test.increaseLamport {
			lamport++
		}
		time.Sleep(50 * time.Millisecond)

		// assert stored value logic
		if test.expectedError == nil {
			getResp, err := client.Get(ctx, &prototypes.GetRequest{Key: test.key, Lamport: uint64(lamport)})
			require.NoError(t, err, "GetMessage should not return error")

			// if test.SetRequest, the value should be stored correctly (assuming that expectedError is not nil when key is out of hashrange)
			// if test.DeleteRequest, the value should be nil
			switch test.request.(type) {
			case *prototypes.SetRequest:
				require.Equal(t,
					partition.ToStoredValue(uint64(lamport-1), value),
					getResp.StoredValue,
					"GetMessage should return correct value",
				)
			case *prototypes.DeleteRequest:
				require.Nil(t, getResp.StoredValue)
			}
			lamport += 1 // for get
		}
	}
}
