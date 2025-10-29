package partition

import (
	"context"

	"github.com/pysel/dkvs/prototypes"
	pbpartition "github.com/pysel/dkvs/prototypes/partition"
)

func (ls *ListenServer) PrepareCommit(ctx context.Context, req *pbpartition.PrepareCommitRequest) (*pbpartition.PrepareCommitResponse, error) {
	if deleteMsg := req.GetDelete(); deleteMsg != nil {
		ls.lockedMessage = deleteMsg
	} else if setMsg := req.GetSet(); setMsg != nil {
		ls.lockedMessage = setMsg
	} else {
		return nil, ErrUnsupported2PCMsg
	}

	ls.EventHandler.Emit(TwoPCPrepareCommitEvent{msg: req.String()})
	return &pbpartition.PrepareCommitResponse{Ok: true}, nil
}

func (ls *ListenServer) AbortCommit(ctx context.Context, req *pbpartition.AbortCommitRequest) (*pbpartition.AbortCommitResponse, error) {
	ls.lockedMessage = nil
	err := ls.Partition.ProcessBacklog()

	ls.EventHandler.Emit(TwoPCAbortEvent{})
	return &pbpartition.AbortCommitResponse{}, err
}

func (ls *ListenServer) Commit(ctx context.Context, req *pbpartition.CommitRequest) (res *pbpartition.CommitResponse, err error) {
	if ls.lockedMessage == nil {
		return nil, ErrNoLockedMessage
	}

	var msgString string
	if deleteMsg, ok := ls.lockedMessage.(*prototypes.DeleteRequest); ok {
		msgString = deleteMsg.String()
		_, err = ls.Delete(ctx, deleteMsg)
	} else if setMsg, ok := ls.lockedMessage.(*prototypes.SetRequest); ok {
		msgString = setMsg.String()
		_, err = ls.Set(ctx, setMsg)
	} else {
		return nil, ErrUnsupported2PCMsg
	}

	if err != nil {
		return nil, err
	}

	ls.lockedMessage = nil

	ls.EventHandler.Emit(TwoPCCommitEvent{msg: msgString})
	return &pbpartition.CommitResponse{}, nil
}
