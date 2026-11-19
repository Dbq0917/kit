package circuitbreaker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

func TestGobreaker(t *testing.T) {
	var (
		breaker          = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{}))
		primeWith        = 100
		shouldPass       = func(n int) bool { return n <= 5 } // https://github.com/sony/gobreaker/blob/bfa846d/gobreaker.go#L76
		circuitOpenError = "circuit breaker is open"
	)
	testFailingEndpoint(t, breaker, primeWith, shouldPass, 0, circuitOpenError)
}

func TestGobreakerIgnoresRateLimitRejections(t *testing.T) {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		// Any counted failure would trip the breaker immediately.
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures > 0 },
	})

	// The endpoint always rejects, as an inner rate limiter would.
	var e endpoint.Endpoint
	e = func(context.Context, interface{}) (interface{}, error) { return nil, ratelimit.ErrLimited }
	e = circuitbreaker.Gobreaker(cb)(e)

	for i := 0; i < 10; i++ {
		if _, err := e(context.Background(), struct{}{}); !errors.Is(err, ratelimit.ErrLimited) {
			t.Fatalf("request %d: want %v, have %v", i, ratelimit.ErrLimited, err)
		}
	}

	if want, have := gobreaker.StateClosed, cb.State(); want != have {
		t.Fatalf("breaker state: want %s, have %s", want, have)
	}
}

func TestGobreakerWithErroringLimiter(t *testing.T) {
	var (
		cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures > 0 },
		})
		limiter = rate.NewLimiter(rate.Every(time.Minute), 2)
		through int
	)

	// Chain the breaker outside of the limiter: breaker(limiter(endpoint)).
	var e endpoint.Endpoint
	e = func(context.Context, interface{}) (interface{}, error) { through++; return struct{}{}, nil }
	e = ratelimit.NewErroringLimiter(limiter)(e)
	e = circuitbreaker.Gobreaker(cb)(e)

	// The first requests pass, the rest are rejected by the limiter.
	for i := 0; i < 10; i++ {
		e(context.Background(), struct{}{})
	}

	if want, have := 2, through; want != have {
		t.Errorf("downstream calls: want %d, have %d", want, have)
	}
	if want, have := gobreaker.StateClosed, cb.State(); want != have {
		t.Errorf("breaker state: want %s, have %s", want, have)
	}
}

func TestGobreakerHalfOpenRecovery(t *testing.T) {
	const timeout = 100 * time.Millisecond

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		ReadyToTrip: func(counts gobreaker.Counts) bool { return counts.ConsecutiveFailures > 0 },
		Timeout:     timeout,
		MaxRequests: 1,
		Interval:    time.Hour, // recovery must not wait for a full interval
	})

	downstreamErr := errors.New("downstream error")
	failing := true
	var e endpoint.Endpoint
	e = func(context.Context, interface{}) (interface{}, error) {
		if failing {
			return nil, downstreamErr
		}
		return struct{}{}, nil
	}
	e = circuitbreaker.Gobreaker(cb)(e)

	// Trip the breaker with a genuine downstream error.
	if _, err := e(context.Background(), struct{}{}); !errors.Is(err, downstreamErr) {
		t.Fatalf("want %v, have %v", downstreamErr, err)
	}
	if want, have := gobreaker.StateOpen, cb.State(); want != have {
		t.Fatalf("breaker state: want %s, have %s", want, have)
	}

	// While open, requests are rejected without reaching the endpoint.
	if _, err := e(context.Background(), struct{}{}); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("want %v, have %v", gobreaker.ErrOpenState, err)
	}

	// After the timeout, a half-open probe is allowed through. A failing
	// probe re-opens the breaker.
	time.Sleep(2 * timeout)
	if _, err := e(context.Background(), struct{}{}); !errors.Is(err, downstreamErr) {
		t.Fatalf("half-open probe: want %v, have %v", downstreamErr, err)
	}
	if _, err := e(context.Background(), struct{}{}); !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("after failed probe: want %v, have %v", gobreaker.ErrOpenState, err)
	}

	// After the next timeout, a successful probe closes the breaker again,
	// without waiting for a full interval cycle.
	failing = false
	time.Sleep(2 * timeout)
	if _, err := e(context.Background(), struct{}{}); err != nil {
		t.Fatalf("half-open probe: unexpected error %v", err)
	}
	if want, have := gobreaker.StateClosed, cb.State(); want != have {
		t.Fatalf("breaker state: want %s, have %s", want, have)
	}
	if _, err := e(context.Background(), struct{}{}); err != nil {
		t.Fatalf("after recovery: unexpected error %v", err)
	}
}
