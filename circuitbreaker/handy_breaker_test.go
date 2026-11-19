package circuitbreaker_test

import (
	"context"
	"errors"
	"testing"

	handybreaker "github.com/streadway/handy/breaker"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

func TestHandyBreaker(t *testing.T) {
	var (
		failureRatio     = 0.05
		breaker          = circuitbreaker.HandyBreaker(handybreaker.NewBreaker(failureRatio))
		primeWith        = handybreaker.DefaultMinObservations * 10
		shouldPass       = func(n int) bool { return (float64(n) / float64(primeWith+n)) <= failureRatio }
		openCircuitError = handybreaker.ErrCircuitOpen.Error()
	)
	testFailingEndpoint(t, breaker, primeWith, shouldPass, 0, openCircuitError)
}

func TestHandyBreakerIgnoresRateLimitRejections(t *testing.T) {
	// Any counted failure would exceed the 0% failure ratio and open the
	// breaker once enough observations have been made.
	breaker := circuitbreaker.HandyBreaker(handybreaker.NewBreaker(0.0))

	// The endpoint always rejects, as an inner rate limiter would.
	var e endpoint.Endpoint
	e = func(context.Context, interface{}) (interface{}, error) { return nil, ratelimit.ErrLimited }
	e = breaker(e)

	for i := 0; i < handybreaker.DefaultMinObservations*2; i++ {
		if _, err := e(context.Background(), struct{}{}); !errors.Is(err, ratelimit.ErrLimited) {
			t.Fatalf("request %d: want %v, have %v", i, ratelimit.ErrLimited, err)
		}
	}
}
