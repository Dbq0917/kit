package circuitbreaker

import (
	"context"
	"errors"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

// Hystrix returns an endpoint.Middleware that implements the circuit
// breaker pattern using the afex/hystrix-go package.
//
// Only errors returned by the wrapped endpoint count against the circuit
// breaker's error count. Errors matching ratelimit.ErrLimited are treated as
// rejections, not failures: the request never reached the downstream service,
// so it doesn't count against the breaker, and the rejection is returned to
// the caller unwrapped.
//
// When using this circuit breaker, please configure your commands separately.
//
// See https://godoc.org/github.com/afex/hystrix-go/hystrix for more
// information.
func Hystrix(commandName string) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			var (
				resp    interface{}
				limited error
			)
			if err := hystrix.Do(commandName, func() (err error) {
				resp, err = next(ctx, request)
				if errors.Is(err, ratelimit.ErrLimited) {
					// Rejected by a rate limiter before reaching
					// the downstream service: don't count it
					// against the breaker, and return the
					// original rejection to the caller.
					limited = err
					return nil
				}
				return err
			}, nil); err != nil {
				return nil, err
			}
			if limited != nil {
				return nil, limited
			}
			return resp, nil
		}
	}
}
