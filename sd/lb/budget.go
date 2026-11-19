package lb

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/go-kit/kit/endpoint"
)

// ErrNoBudget is returned by a budget-aware retry endpoint when neither a
// TotalTimeout is configured nor a deadline exists on the incoming context.
// A unified budget cannot be derived, so no attempt is made.
var ErrNoBudget = errors.New("retry: no timeout budget: provide TotalTimeout or call with a deadline-bearing context")

// Backoff determines how long a BudgetRetry waits before the next attempt.
// Implementations must be safe for concurrent use.
type Backoff interface {
	// Next returns the delay before attempt n+1, where n is the
	// zero-based index of the attempt that just failed.
	Next(n int) time.Duration
}

// ExponentialBackoff implements "full jitter" exponential backoff, as
// described in the AWS Architecture Blog article "Exponential Backoff And
// Jitter". Each delay is drawn uniformly from [0, base*2^n), capped at
// Max. Full jitter spreads retries across clients and prevents synchronized
// retry storms from hammering a recovering downstream.
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration

	mu  sync.Mutex
	rng *rand.Rand
}

// Next implements Backoff.
func (b *ExponentialBackoff) Next(n int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rng == nil {
		b.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	ceiling := b.Base
	for i := 0; i < n && ceiling < b.Max; i++ {
		if doubled := ceiling * 2; doubled < ceiling || doubled > b.Max {
			ceiling = b.Max
			break
		} else {
			ceiling = doubled
		}
	}
	if ceiling > b.Max {
		ceiling = b.Max
	}
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(b.rng.Int63n(int64(ceiling)))
}

// BudgetConfig configures RetryWithBudget. Unlike Retry, which owns a
// timeout independent of the caller's deadline, a budget-aware retry shares
// one end-to-end deadline with the timeout middleware (or the incoming
// context) and subdivides it across attempts, so that the total load a
// single user request can place on a downstream stays bounded.
type BudgetConfig struct {
	// MaxAttempts is the hard cap on the number of endpoint invocations,
	// including the first attempt. It bounds amplification even when the
	// total budget would allow more retries.
	MaxAttempts int

	// TotalTimeout is the overall deadline for every attempt combined. When
	// zero, the deadline of the incoming context is used. It is an error to
	// leave both unset. If the context already has an earlier deadline, the
	// tighter of the two always wins.
	TotalTimeout time.Duration

	// PerAttemptTimeout caps the timeout handed to a single attempt. The
	// actual per-attempt timeout is min(remaining budget / attempts left,
	// PerAttemptTimeout). Zero means no separate cap.
	PerAttemptTimeout time.Duration

	// Backoff inserts a jittered delay between attempts. The delay counts
	// against the total budget, so a long backoff naturally suppresses
	// further retries when the budget is nearly spent. nil means retry
	// immediately.
	Backoff Backoff

	// IsRetryable decides whether an error from an attempt qualifies for a
	// retry. Non-retryable errors (business failures, bad requests, circuit
	// open) are returned immediately without spending more budget. nil means
	// every error is retried, matching the behavior of Retry.
	IsRetryable func(error) bool
}

// RetryWithBudget wraps a load balancer with a retry endpoint whose timeout
// budget is unified with the caller's deadline.
//
// The recommended composition is a timeout middleware on the outside,
// establishing the end-to-end deadline, followed by RetryWithBudget:
//
//	e := endpoint.Chain(
//		endpoint.Timeout(500*time.Millisecond), // single, total deadline
//	)(lb.RetryWithBudget(lb.BudgetConfig{
//		MaxAttempts:       3,
//		PerAttemptTimeout: 200 * time.Millisecond,
//		Backoff:           &lb.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond},
//		IsRetryable:       isTransient,
//	}, balancer))
//
// With that chain, one user request produces at most MaxAttempts downstream
// calls and cannot outlive the 500ms deadline: each attempt receives a slice
// of the remaining budget rather than a fresh, independent timeout.
func RetryWithBudget(cfg BudgetConfig, b Balancer) endpoint.Endpoint {
	if b == nil {
		panic("nil Balancer")
	}
	if cfg.MaxAttempts < 1 {
		panic("BudgetConfig: MaxAttempts must be at least 1")
	}
	if cfg.TotalTimeout < 0 {
		panic("BudgetConfig: TotalTimeout must not be negative")
	}
	if cfg.PerAttemptTimeout < 0 {
		panic("BudgetConfig: PerAttemptTimeout must not be negative")
	}

	return func(ctx context.Context, request interface{}) (interface{}, error) {
		budgetCtx := ctx
		if cfg.TotalTimeout > 0 {
			var cancel context.CancelFunc
			budgetCtx, cancel = context.WithTimeout(ctx, cfg.TotalTimeout)
			defer cancel()
		} else if _, ok := budgetCtx.Deadline(); !ok {
			return nil, RetryError{Final: ErrNoBudget}
		}

		final := RetryError{}
		for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
			if err := budgetCtx.Err(); err != nil {
				final.RawErrors = append(final.RawErrors, err)
				final.Final = err
				return nil, final
			}

			attemptCtx, cancel := context.WithTimeout(budgetCtx, attemptTimeout(budgetCtx, cfg, attempt))
			response, err := invokeOnce(attemptCtx, b, request)
			cancel()
			if err == nil {
				return response, nil
			}

			final.RawErrors = append(final.RawErrors, err)
			if cfg.IsRetryable != nil && !cfg.IsRetryable(err) {
				final.Final = err
				return nil, final
			}
			if attempt == cfg.MaxAttempts {
				break
			}
			if !waitBackoff(budgetCtx, cfg.Backoff, attempt-1) {
				break
			}
		}

		final.Final = budgetCtx.Err()
		if final.Final == nil {
			final.Final = final.RawErrors[len(final.RawErrors)-1]
		}
		return nil, final
	}
}

// attemptTimeout divides the remaining budget evenly over the attempts still
// available and applies the optional per-attempt cap. Sharing the parent
// deadline means an attempt that overruns is cut off by the total budget even
// if its own slice would nominally be longer.
func attemptTimeout(ctx context.Context, cfg BudgetConfig, attempt int) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return cfg.PerAttemptTimeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}

	attemptsLeft := cfg.MaxAttempts - attempt + 1
	slice := remaining / time.Duration(attemptsLeft)
	if cfg.PerAttemptTimeout > 0 && slice > cfg.PerAttemptTimeout {
		slice = cfg.PerAttemptTimeout
	}
	return slice
}

// invokeOnce runs one balanced endpoint invocation. It is executed in a
// goroutine so that an endpoint ignoring context cancellation can be
// abandoned when its attempt context expires, mirroring Retry.
func invokeOnce(ctx context.Context, b Balancer, request interface{}) (interface{}, error) {
	type result struct {
		response interface{}
		err      error
	}
	results := make(chan result, 1)
	go func() {
		e, err := b.Endpoint()
		if err != nil {
			results <- result{err: err}
			return
		}
		response, err := e(ctx, request)
		results <- result{response: response, err: err}
	}()

	select {
	case res := <-results:
		return res.response, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// waitBackoff sleeps for the backoff duration while remaining interruptible
// by budget exhaustion or cancellation. It reports whether another attempt
// may proceed.
func waitBackoff(ctx context.Context, backoff Backoff, n int) bool {
	if backoff == nil {
		return ctx.Err() == nil
	}
	delay := backoff.Next(n)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
