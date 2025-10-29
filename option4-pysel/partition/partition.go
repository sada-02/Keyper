package partition

import (
	"fmt"
	"sync"

	db "github.com/pysel/dkvs/db"
	hashrange "github.com/pysel/dkvs/types/hashrange"

	leveldb "github.com/pysel/dkvs/db/leveldb"
	"github.com/pysel/dkvs/prototypes"
	"github.com/pysel/dkvs/shared"
	"github.com/pysel/dkvs/types"
	"google.golang.org/protobuf/proto"
)

// Partition is a node that is responsible for some range of keys.
type Partition struct {
	// hashrange is a range of keys that this partition is responsible for.
	hashrange *hashrange.Range

	// Database instance
	db.DB

	// read-write mutex
	rwmutex sync.RWMutex

	// set of messages that could not have been processed yet for some reason.
	backlog *types.Backlog

	// timestamp of the last message that was processed.
	timestamp uint64

	// message that this partition is currently locked in two-phase commit prepare step.
	lockedMessage proto.Message

	// event handler
	EventHandler *shared.EventHandler
}

// NewPartition creates a new partition instance.
func NewPartition(dbPath string) *Partition {
	db, err := leveldb.NewLevelDB(dbPath)
	if err != nil {
		panic(err)
	}

	eventHandler := shared.NewEventHandler()
	return &Partition{
		hashrange:    nil, // balancer should set this
		DB:           db,
		rwmutex:      sync.RWMutex{},
		timestamp:    0,
		backlog:      types.NewBacklog(),
		EventHandler: eventHandler,
	}
}

// ---- Database methods ----
// Keys should be sent of 32 length bytes, since SHA-2 produces 256-bit hashes, and be of big endian format.

func (p *Partition) Get(key []byte) ([]byte, error) {
	shaKey := types.ShaKey(key)

	if err := p.checkKeyRange(shaKey[:]); err != nil {
		return nil, ErrNotThisPartitionKey
	}

	return p.DB.Get(key)
}

func (p *Partition) Set(key, value []byte) error {
	shaKey := types.ShaKey(key)

	if err := p.checkKeyRange(shaKey[:]); err != nil {
		return ErrNotThisPartitionKey
	}

	return p.DB.Set(key, value)
}

func (p *Partition) Delete(key []byte) error {
	shaKey := types.ShaKey(key)

	if err := p.checkKeyRange(shaKey[:]); err != nil {
		return ErrNotThisPartitionKey
	}

	return p.DB.Delete(key)
}

func (p *Partition) Close() error {
	return p.DB.Close()
}

func (p *Partition) SetHashrange(hashrange *hashrange.Range) {
	p.hashrange = hashrange
}

// validate TS checks the timestamp of received message against local timestamp
func (p *Partition) validateTS(ts uint64) error {
	if ts <= p.timestamp {
		return ErrTimestampIsStale{CurrentTimestamp: p.timestamp, StaleTimestamp: ts}
	} else if ts > p.timestamp+1 { // timestamp is not the next one
		return ErrTimestampNotNext{CurrentTimestamp: p.timestamp, ReceivedTimestamp: ts}
	}

	return nil
}

func (p *Partition) IncrTs() {
	p.timestamp++
}

// ProcessBacklog processes messages in backlog.
func (p *Partition) ProcessBacklog() error {
	var latestTimestamp uint64
	for {
		// check if the partition is ready to process the next message
		if p.backlog.GetSmallestTimestamp() > p.timestamp+1 {
			break
		}

		_, message := p.backlog.Pop()
		if message == nil {
			break
		}

		var err error
		var messageType string
		var messageKey string
		var messageValue string
		switch m := message.(type) {
		case *prototypes.SetRequest:
			latestTimestamp = m.Lamport
			shaKey := types.ShaKey(m.Key)
			err = p.Set(shaKey[:], m.Value)

			messageType, messageKey, messageValue = "set", string(m.Key), string(m.Value)
		case *prototypes.DeleteRequest:
			latestTimestamp = m.Lamport
			shaKey := types.ShaKey(m.Key)
			err = p.Delete(shaKey[:])

			messageType, messageKey = "delete", string(m.Key)
		default:
			fmt.Println("Unknown message type") // TODO: think of something better here.
		}
		p.EventHandler.Emit(NewBacklogMessageProcessedEvent(messageType, messageKey, messageValue))

		if err != nil {
			return err
		}
	}

	if latestTimestamp != 0 { // aka: if some message was processed
		p.timestamp = latestTimestamp
	}

	return nil
}
