package circuitbreaker

import (
	"context"
	"errors"

	"github.com/sony/gobreaker"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

// Gobreaker returns an endpoint.Middleware that implements the circuit
// breaker pattern using the sony/gobreaker package. Only errors returned by
// the wrapped endpoint count against the circuit breaker's error count.
//
// Errors matching ratelimit.ErrLimited are treated as rejections, not
// failures: the request never reached the downstream service, so it doesn't
// count against the breaker, and the rejection is returned to the caller
// unwrapped. Rate limit rejections therefore can't trip the breaker, no
// matter how the two middlewares are ordered in the chain.
//
// Recovery is governed by the breaker's settings: after Settings.Timeout in
// the open state, the breaker becomes half-open and allows up to
// Settings.MaxRequests probe requests through, closing again when they
// succeed. It doesn't wait for a full Settings.Interval cycle to recover.
//
// See http://godoc.org/github.com/sony/gobreaker for more information.
func Gobreaker(cb *gobreaker.CircuitBreaker) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			var limited error
			response, err := cb.Execute(func() (interface{}, error) {
				response, err := next(ctx, request)
				if errors.Is(err, ratelimit.ErrLimited) {
					// The request was rejected by a rate limiter
					// before reaching the downstream service.
					// Report a neutral result to the breaker, and
					// return the original rejection to the caller.
					limited = err
					return response, nil
				}
				return response, err
			})
			if limited != nil {
				return response, limited
			}
			return response, err
		}
	}
}
