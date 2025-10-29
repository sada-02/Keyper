package partition

import (
	"fmt"
	"math/big"

	"github.com/pysel/dkvs/shared"
)

type (
	SetHashrangeEvent struct {
		min *big.Int
		max *big.Int
	}

	SetEvent struct {
		key  string
		data string
	}

	GetEvent struct {
		key      string
		returned string
	}

	DeleteEvent struct {
		key string
	}

	StaleRequestEvent struct {
		req               string
		currentTimestamp  uint64
		receivedTimestamp uint64
	}

	NotNextRequestEvent struct {
		req               string
		currentTimestamp  uint64
		receivedTimestamp uint64
	}

	ServerStartEvent struct {
		port uint64
	}

	TwoPCPrepareCommitEvent struct {
		msg string
	}

	TwoPCAbortEvent struct{}

	TwoPCCommitEvent struct {
		msg string
	}

	BacklogMessageProcessedEvent struct {
		msg string
	}
)

func (e SetEvent) Severity() string {
	return "info"
}

func (e SetEvent) Message() string {
	return fmt.Sprintf("Stored a message: %s -> %s", shared.GreenWrap(e.key), shared.GreenWrap(e.data))
}

func (e GetEvent) Severity() string {
	return "info"
}

func (e GetEvent) Message() string {
	return fmt.Sprintf("Retrieved a message: %s -> %s", shared.GreenWrap(e.key), shared.GreenWrap(e.returned))
}

func (e DeleteEvent) Severity() string {
	return "info"
}

func (e DeleteEvent) Message() string {
	return fmt.Sprintf("Deleted a message: %s", shared.GreenWrap(e.key))
}

func (e StaleRequestEvent) Severity() string {
	return "warning"
}

func (e StaleRequestEvent) Message() string {
	return fmt.Sprintf("\033[33mStale Request\033[0m. Request: {%s}. Current timestamp: \033[32m%d\033[0m, received timestamp: \033[32m%d\033[0m", shared.GreyWrap(e.req), e.currentTimestamp, e.receivedTimestamp)
}

func (e NotNextRequestEvent) Severity() string {
	return "warning"
}

func (e NotNextRequestEvent) Message() string {
	return fmt.Sprintf("\033[33mFuture Request\033[0m. Request: {%s}. Current timestamp: \033[32m%d\033[0m, received timestamp: \033[32m%d\033[0m", shared.GreyWrap(e.req), e.currentTimestamp, e.receivedTimestamp)
}

func (e SetHashrangeEvent) Severity() string {
	return "info"
}

func (e SetHashrangeEvent) Message() string {
	return fmt.Sprintf("Set hashrange: %s -> %s", shared.GreenWrap(e.min.String()), shared.GreenWrap(e.max.String()))
}

func (e ServerStartEvent) Severity() string {
	return "info"
}

func (e ServerStartEvent) Message() string {
	return fmt.Sprintf("Server started on port: \033[32m%d\033[0m", e.port)
}

// ----------------- 2PC Events -----------------

func (e TwoPCPrepareCommitEvent) Severity() string {
	return "info"
}

func (e TwoPCPrepareCommitEvent) Message() string {
	return fmt.Sprintf("2PC prepare commit locked message: %s", shared.GreenWrap(e.msg))
}

func (e TwoPCAbortEvent) Severity() string {
	return "warning"
}

func (e TwoPCAbortEvent) Message() string {
	return shared.YellowWrap("2PC was aborted")
}

func (e TwoPCCommitEvent) Severity() string {
	return "info"
}

func (e TwoPCCommitEvent) Message() string {
	return fmt.Sprintf("2PC commit of locked message: %s", shared.GreenWrap(e.msg))
}

func NewBacklogMessageProcessedEvent(type_, key, value string) BacklogMessageProcessedEvent {
	if type_ == "set" {
		return BacklogMessageProcessedEvent{fmt.Sprintf("Set: %s -> %s", key, value)}
	} else if type_ == "delete" {
		return BacklogMessageProcessedEvent{fmt.Sprintf("Delete: %s", key)}
	}

	return BacklogMessageProcessedEvent{fmt.Sprintf("Unknown: %s", type_)}
}

func (e BacklogMessageProcessedEvent) Severity() string {
	return "warning"
}

func (e BacklogMessageProcessedEvent) Message() string {
	return fmt.Sprintf("Backlog message processed: %s", shared.GreenWrap(e.msg))
}
