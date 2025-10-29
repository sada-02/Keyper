package shared

import "fmt"

type (
	ErrorEvent struct {
		Req string
		Err error
	}
)

func (e ErrorEvent) Severity() string {
	return "error"
}

func (e ErrorEvent) Message() string {
	return fmt.Sprintf("Error for request %s: %s", GreyWrap(e.Req), RedWrap(e.Err.Error()))
}
