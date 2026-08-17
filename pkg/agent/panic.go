package agent

import (
	"fmt"
	"runtime/debug"
)

// panicStackLimit bounds the stack captured in a panic error. The trace is
// forwarded to the backend as a log message and an error payload, so it has to
// stay small enough not to blow up those requests.
const panicStackLimit = 4096

// PanicError converts a recovered panic value into an error carrying the stack
// of the panicking goroutine. It must be called from within the deferred
// function that recovered, otherwise the stack no longer points at the panic.
func PanicError(name string, recovered any) error {
	stack := debug.Stack()
	if len(stack) > panicStackLimit {
		stack = stack[:panicStackLimit]
	}
	return fmt.Errorf("panic in %s: %v\n%s", name, recovered, stack)
}
