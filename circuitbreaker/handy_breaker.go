package circuitbreaker

import (
	"context"
	"errors"
	"time"

	"github.com/streadway/handy/breaker"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

// HandyBreaker returns an endpoint.Middleware that implements the circuit
// breaker pattern using the streadway/handy/breaker package. Only errors
// returned by the wrapped endpoint count against the circuit breaker's error
// count.
//
// Errors matching ratelimit.ErrLimited are treated as rejections, not
// failures: the request never reached the downstream service, so it's
// reported to the breaker as neither a success nor a failure, and the
// rejection is returned to the caller unwrapped. Rate limit rejections
// therefore can't trip the breaker, no matter how the two middlewares are
// ordered in the chain.
//
// See http://godoc.org/github.com/streadway/handy/breaker for more
// information.
func HandyBreaker(cb breaker.Breaker) endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (response interface{}, err error) {
			if !cb.Allow() {
				return nil, breaker.ErrCircuitOpen
			}

			defer func(begin time.Time) {
				switch {
				case err == nil:
					cb.Success(time.Since(begin))
				case errors.Is(err, ratelimit.ErrLimited):
					// Rejected by a rate limiter before reaching
					// the downstream service: report nothing.
				default:
					cb.Failure(time.Since(begin))
				}
			}(time.Now())

			response, err = next(ctx, request)
			return
		}
	}
}
