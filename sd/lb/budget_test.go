package lb_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/sd"
	"github.com/go-kit/kit/sd/lb"
)

func TestRetryWithBudgetCapsAmplificationAtMaxAttempts(t *testing.T) {
	var calls int32
	e := func(context.Context, interface{}) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  3,
		TotalTimeout: time.Second,
	}, balancer)

	_, err := retry(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("want 3 downstream calls (amplification cap), have %d", got)
	}
}

func TestRetryWithBudgetBoundedByTotalTimeout(t *testing.T) {
	var calls int32
	e := func(ctx context.Context, _ interface{}) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		<-ctx.Done() // every attempt runs until its slice of the budget expires
		return nil, ctx.Err()
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  50,
		TotalTimeout: 50 * time.Millisecond,
	}, balancer)

	start := time.Now()
	_, err := retry(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Errorf("retry outlived total budget by %v", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got >= 50 {
		t.Errorf("expected budget exhaustion to stop retries early, got %d calls", got)
	}
}

func TestRetryWithBudgetSubdividesRemainingDeadline(t *testing.T) {
	deadlines := make(chan time.Duration, 4)
	e := func(ctx context.Context, _ interface{}) (interface{}, error) {
		if d, ok := ctx.Deadline(); ok {
			deadlines <- time.Until(d)
		}
		return nil, errors.New("boom")
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  3,
		TotalTimeout: 300 * time.Millisecond,
	}, balancer)

	_, _ = retry(context.Background(), struct{}{})

	first := <-deadlines
	if first > 110*time.Millisecond {
		t.Errorf("first attempt should receive ~1/3 of the budget, have %v", first)
	}
	second := <-deadlines
	// A fast failure barely spends the budget, so the leftover is re-divided
	// across the two remaining attempts: the slice grows toward 150ms, but
	// can never exceed the remaining total budget (~300ms).
	if second < 130*time.Millisecond || second > 305*time.Millisecond {
		t.Errorf("second attempt slice %v outside expected ~150ms re-division", second)
	}
	third := <-deadlines
	if third > 305*time.Millisecond {
		t.Errorf("final attempt %v must still be bounded by the total budget", third)
	}
}

func TestRetryWithBudgetHonorsPerAttemptCap(t *testing.T) {
	got := make(chan time.Duration, 1)
	e := func(ctx context.Context, _ interface{}) (interface{}, error) {
		if d, ok := ctx.Deadline(); ok {
			got <- time.Until(d)
		}
		return nil, errors.New("boom")
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:       4,
		TotalTimeout:      time.Second,
		PerAttemptTimeout: 20 * time.Millisecond,
	}, balancer)

	_, _ = retry(context.Background(), struct{}{})
	if d := <-got; d > 25*time.Millisecond {
		t.Errorf("per-attempt timeout should be capped near 20ms, have %v", d)
	}
}

func TestRetryWithBudgetUsesIncomingContextDeadline(t *testing.T) {
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{
		func(context.Context, interface{}) (interface{}, error) { return nil, errors.New("boom") },
	})
	retry := lb.RetryWithBudget(lb.BudgetConfig{MaxAttempts: 3}, balancer)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := retry(ctx, struct{}{})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("retry ignored incoming deadline, took %v", elapsed)
	}
}

func TestRetryWithBudgetRequiresABudget(t *testing.T) {
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{
		func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil },
	})
	retry := lb.RetryWithBudget(lb.BudgetConfig{MaxAttempts: 3}, balancer)

	if _, err := retry(context.Background(), struct{}{}); err == nil {
		t.Fatal("expected ErrNoBudget, got none")
	} else if retryErr, ok := err.(lb.RetryError); !ok || retryErr.Final != lb.ErrNoBudget {
		t.Fatalf("want ErrNoBudget, have %v", err)
	}
}

func TestRetryWithBudgetDoesNotRetryNonRetryableErrors(t *testing.T) {
	var calls int32
	badRequest := errors.New("bad request")
	e := func(context.Context, interface{}) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, badRequest
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  5,
		TotalTimeout: time.Second,
		IsRetryable:  func(err error) bool { return err != badRequest },
	}, balancer)

	_, err := retry(context.Background(), struct{}{})
	retryErr, ok := err.(lb.RetryError)
	if !ok || retryErr.Final != badRequest {
		t.Fatalf("want badRequest as final error, have %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("non-retryable error must not be retried, have %d calls", got)
	}
}

type fixedBackoff struct{ delays []time.Duration }

func (f *fixedBackoff) Next(n int) time.Duration {
	if n >= len(f.delays) {
		return f.delays[len(f.delays)-1]
	}
	return f.delays[n]
}

func TestRetryWithBudgetBackoffConsumesTheBudget(t *testing.T) {
	var calls int32
	e := func(context.Context, interface{}) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  4,
		TotalTimeout: 50 * time.Millisecond,
		Backoff:      &fixedBackoff{delays: []time.Duration{30 * time.Millisecond, 30 * time.Millisecond}},
	}, balancer)

	start := time.Now()
	_, err := retry(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if elapsed := time.Since(start); elapsed > 120*time.Millisecond {
		t.Errorf("backoff + attempts exceeded total budget, took %v", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("backoff should leave budget for 2 attempts only, have %d", got)
	}
}

func TestRetryWithBudgetSucceedsOnLaterAttempt(t *testing.T) {
	var calls int32
	e := func(context.Context, interface{}) (interface{}, error) {
		if atomic.AddInt32(&calls, 1) < 3 {
			return nil, errors.New("transient")
		}
		return struct{}{}, nil
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts:  3,
		TotalTimeout: time.Second,
		Backoff:      &fixedBackoff{delays: []time.Duration{time.Millisecond}},
	}, balancer)

	if _, err := retry(context.Background(), struct{}{}); err != nil {
		t.Fatalf("expected success on third attempt, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("want 3 attempts, have %d", got)
	}
}

func TestTimeoutOutsideRetryWithBudgetSharesOneDeadline(t *testing.T) {
	var calls int32
	var (
		mu               sync.Mutex
		attemptDeadlines []time.Time
	)
	e := func(ctx context.Context, _ interface{}) (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		if d, ok := ctx.Deadline(); ok {
			mu.Lock()
			attemptDeadlines = append(attemptDeadlines, d)
			mu.Unlock()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	balancer := lb.NewRoundRobin(sd.FixedEndpointer{e})
	retry := lb.RetryWithBudget(lb.BudgetConfig{
		MaxAttempts: 10,
	}, balancer)
	chain := endpoint.Chain(endpoint.Timeout(40 * time.Millisecond))(retry)

	start := time.Now()
	_, err := chain(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected timeout error, got none")
	}
	if elapsed := time.Since(start); elapsed > 120*time.Millisecond {
		t.Errorf("chain outlived the outer timeout, took %v", elapsed)
	}
	got := atomic.LoadInt32(&calls)
	if got == 0 || got > 10 {
		t.Errorf("amplification must stay within [1, MaxAttempts], have %d", got)
	}
	// The key property: no attempt receives a fresh, independent timeout.
	// Every per-attempt deadline is at or before the outer 40ms deadline.
	outerDeadline := start.Add(40 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, d := range attemptDeadlines {
		if d.After(outerDeadline.Add(5 * time.Millisecond)) {
			t.Fatalf("attempt deadline %v escapes the unified outer deadline %v", d, outerDeadline)
		}
	}
}

func TestExponentialBackoffStaysWithinBounds(t *testing.T) {
	b := &lb.ExponentialBackoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond}
	for n := 0; n < 10; n++ {
		d := b.Next(n)
		if d < 0 || d > 100*time.Millisecond {
			t.Fatalf("backoff %v outside [0, 100ms] at n=%d", d, n)
		}
	}
}
