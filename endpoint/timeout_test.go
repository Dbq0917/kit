package endpoint_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/kit/endpoint"
)

func TestTimeoutSucceedsWhenEndpointIsFast(t *testing.T) {
	want := struct{}{}
	e := endpoint.Timeout(time.Millisecond)(func(context.Context, interface{}) (interface{}, error) {
		return want, nil
	})

	have, err := e(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if have != want {
		t.Errorf("want %v, have %v", want, have)
	}
}

func TestTimeoutReturnsConfiguredErrorWhenDeadlineElapses(t *testing.T) {
	step := make(chan struct{})
	defer close(step)
	e := endpoint.Timeout(time.Millisecond)(func(ctx context.Context, _ interface{}) (interface{}, error) {
		<-step // ignores cancellation, must be abandoned
		return struct{}{}, nil
	})

	start := time.Now()
	_, err := e(context.Background(), struct{}{})
	if err != endpoint.ErrTimeout {
		t.Fatalf("want %v, have %v", endpoint.ErrTimeout, err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("middleware waited %v for an endpoint ignoring cancellation", elapsed)
	}
}

func TestTimeoutPropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := endpoint.Timeout(time.Second)(func(context.Context, interface{}) (interface{}, error) {
		return struct{}{}, nil
	})

	if _, err := e(ctx, struct{}{}); err != context.Canceled {
		t.Fatalf("want %v, have %v", context.Canceled, err)
	}
}

func TestTimeoutWithErrorUsesCustomError(t *testing.T) {
	myErr := errors.New("custom timeout")
	step := make(chan struct{})
	defer close(step)
	e := endpoint.TimeoutWithError(time.Millisecond, myErr)(
		func(context.Context, interface{}) (interface{}, error) { <-step; return nil, nil },
	)

	if _, err := e(context.Background(), struct{}{}); err != myErr {
		t.Fatalf("want %v, have %v", myErr, err)
	}
}

func TestTimeoutPanicsOnNonPositiveTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for zero timeout")
		}
	}()
	endpoint.Timeout(0)
}
