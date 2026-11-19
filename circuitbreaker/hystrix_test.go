package circuitbreaker_test

import (
	"context"
	"errors"
	"io/ioutil"
	stdlog "log"
	"testing"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/ratelimit"
)

func TestHystrix(t *testing.T) {
	stdlog.SetOutput(ioutil.Discard)

	const (
		commandName   = "my-endpoint"
		errorPercent  = 5
		maxConcurrent = 1000
	)
	hystrix.ConfigureCommand(commandName, hystrix.CommandConfig{
		ErrorPercentThreshold: errorPercent,
		MaxConcurrentRequests: maxConcurrent,
	})

	var (
		breaker          = circuitbreaker.Hystrix(commandName)
		primeWith        = hystrix.DefaultVolumeThreshold * 2
		shouldPass       = func(n int) bool { return (float64(n) / float64(primeWith+n)) <= (float64(errorPercent-1) / 100.0) }
		openCircuitError = hystrix.ErrCircuitOpen.Error()
	)

	// hystrix-go uses buffered channels to receive reports on request success/failure,
	// and so is basically impossible to test deterministically. We have to make sure
	// the report buffer is emptied, by injecting a sleep between each invocation.
	requestDelay := 5 * time.Millisecond

	testFailingEndpoint(t, breaker, primeWith, shouldPass, requestDelay, openCircuitError)
}

func TestHystrixIgnoresRateLimitRejections(t *testing.T) {
	stdlog.SetOutput(ioutil.Discard)

	const commandName = "ratelimited-endpoint"
	hystrix.ConfigureCommand(commandName, hystrix.CommandConfig{
		// Any counted failure would push the error percentage to 100% and
		// open the breaker once the volume threshold is met.
		ErrorPercentThreshold: 1,
		MaxConcurrentRequests: 1000,
	})

	// The endpoint always rejects, as an inner rate limiter would.
	var e endpoint.Endpoint
	e = func(context.Context, interface{}) (interface{}, error) { return nil, ratelimit.ErrLimited }
	e = circuitbreaker.Hystrix(commandName)(e)

	// hystrix-go reports results on buffered channels, so inject a small
	// delay to let the breaker observe them, as in TestHystrix.
	for i := 0; i < hystrix.DefaultVolumeThreshold*2; i++ {
		if _, err := e(context.Background(), struct{}{}); !errors.Is(err, ratelimit.ErrLimited) {
			t.Fatalf("request %d: want %v, have %v", i, ratelimit.ErrLimited, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
