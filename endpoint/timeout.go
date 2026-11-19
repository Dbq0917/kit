package endpoint

import (
	"context"
	"errors"
	"time"
)

// ErrTimeout is returned by the Timeout middleware when the wrapped endpoint
// does not return within the configured timeout.
var ErrTimeout = errors.New("endpoint timeout exceeded")

// Timeout returns a Middleware that bounds every invocation of the wrapped
// endpoint to the given timeout. The deadline is enforced through a child
// context, so endpoints that honor context cancellation are interrupted
// promptly; endpoints that ignore the context are abandoned rather than being
// waited for, and ErrTimeout is returned to the caller.
//
// Timeout is intended to sit outside of a budget-aware retry middleware: it
// establishes the single, end-to-end deadline that the retry layer then
// subdivides across attempts. See sd/lb.RetryWithBudget for details.
func Timeout(timeout time.Duration) Middleware {
	return TimeoutWithError(timeout, ErrTimeout)
}

// TimeoutWithError behaves like Timeout but allows the caller to specify the
// error returned on timeout.
func TimeoutWithError(timeout time.Duration, err error) Middleware {
	if timeout <= 0 {
		panic("Timeout: timeout must be greater than zero")
	}
	if err == nil {
		err = ErrTimeout
	}
	return func(next Endpoint) Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			var (
				response interface{}
				respErr  error
				finished = make(chan struct{})
			)
			go func() {
				defer close(finished)
				response, respErr = next(ctx, request)
			}()

			select {
			case <-finished:
				return response, respErr
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil, err
				}
				return nil, ctx.Err()
			}
		}
	}
}
