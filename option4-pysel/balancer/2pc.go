package balancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/pysel/dkvs/balancer/rangeview"
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
	"github.com/pysel/dkvs/types/hashrange"
)

// AtomicMessage sends a message to all partitions that are responsible for the given key and awaits for their responses.
// On successfull ack from all nodes, sends a commit message, else sends an abort message.
func (b *Balancer) AtomicMessage(ctx context.Context, range_ *hashrange.Range, msg *pbpartition.PrepareCommitRequest) error {
	rangeView := b.rangeToViews[range_.AsKey()]
	if len(rangeView.Clients) == 0 {
		return ErrRangeNotYetCovered
	}

	// synchronous prepare commit step
	err := b.prepareCommit(rangeView, msg)
	// If >= 1 partition aborted, abort all
	// Before aborting/committing, save decision to disk so that we can recover from a crash
	if err != nil {
		err := b.DB.Set(PrepareCommitDecisionKey, []byte("abort"))
		if err != nil {
			return ErrDecisionNotSavedToDisk{Reason: err, Decision: []byte("abort")}
		}

		b.abortCommit(ctx, rangeView)
	} else {
		err := b.DB.Set(PrepareCommitDecisionKey, []byte("commit"))
		if err != nil {
			return ErrDecisionNotSavedToDisk{Reason: err, Decision: []byte("commit")}
		}

		err = b.commit(ctx, rangeView)
		if err != nil {
			return err
		}
	}

	err = b.DB.Delete(PrepareCommitDecisionKey)
	if err != nil {
		return ErrDecisionWasNotCleared{Reason: err}
	}

	return nil
}

// prepareCommit sends a prepare commit request to all partitions that are responsible for the given key and awaits for their responses.
func (b *Balancer) prepareCommit(rangeView *rangeview.RangeView, msg *pbpartition.PrepareCommitRequest) error {
	var wg sync.WaitGroup
	channel := make(chan error, len(rangeView.Clients))
	for _, client := range rangeView.Clients {
		wg.Add(1)
		clientCopy := *client

		go func() {
			defer wg.Done()
			resp, err := clientCopy.PrepareCommit(context.Background(), msg)
			if err != nil {
				channel <- err
			}

			if resp != nil && resp.Ok {
				channel <- nil
			} else {
				channel <- ErrPrepareCommitAborted
			}
		}()
	}

	wg.Wait()

	for i := 0; i < len(rangeView.Clients); i++ {
		err := <-channel
		if err != nil {
			return err
		}
	}

	return nil
}

// commit sends a commit request to provided partitions.
func (b *Balancer) commit(ctx context.Context, rangeView *rangeview.RangeView) error {
	var wg sync.WaitGroup
	channel := make(chan error, len(rangeView.Clients))
	for _, client := range rangeView.Clients {
		wg.Add(1)
		clientCopy := *client

		go func() {
			defer wg.Done()
			_, err := clientCopy.Commit(ctx, &pbpartition.CommitRequest{})
			if err != nil {
				channel <- err
			} else {
				channel <- nil
			}
		}()
	}

	wg.Wait()

	success := 0
	for i := 0; i < len(rangeView.Clients); i++ {
		if err := <-channel; err != nil {
			return ErrCommitAborted{Err: err}
		}
		success++
	}

	if success == len(rangeView.Clients) {
		rangeView.Lamport++
	}

	return nil
}

// abortCommit sends an abort commit request to provided partitions.
func (b *Balancer) abortCommit(ctx context.Context, rangeView *rangeview.RangeView) {
	var wg sync.WaitGroup
	channel := make(chan error, len(rangeView.Clients))
	for _, client := range rangeView.Clients {
		wg.Add(1)
		clientCopy := *client

		go func() {
			defer wg.Done()
			_, err := clientCopy.AbortCommit(ctx, &pbpartition.AbortCommitRequest{})
			if err != nil {
				// TODO: some error handling here
				fmt.Println(err, "TODO: Unimplemented branch 1")
			}

			channel <- nil
		}()
	}

	wg.Wait()

	for i := 0; i < len(rangeView.Clients); i++ {
		if <-channel != nil {
			fmt.Println("TODO: Unimplemented branch 2")
			return
		}
	}
}
