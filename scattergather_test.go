package scattergather

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestScatteredError(t *testing.T) {
	var e *ScatteredError
	assert.False(t, e.HasErrors(), "nil ScatteredError has no errors")
	e = &ScatteredError{}
	assert.False(t, e.HasErrors(), "empty ScatteredError has no errors")
}

func TestBasic(t *testing.T) {
	sg := new(ScatterGather[int])
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args))
	for i := range args {
		args[i] = i
		expected[i] = i * i
		sg.Run(ctx, square(i))
	}
	result, err := sg.Wait()
	assert.Nil(t, err, "No error is returned")
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func TestWithErrors(t *testing.T) {
	sg := New[int](0)
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args)/2)
	expecterr := &ScatteredError{}
	for i := range args {
		args[i] = i
		if i%2 != 0 {
			expected[i/2] = i * i
		} else {
			expecterr.AddError(&cantEven{})
		}
		sg.Run(ctx, squareOdds(i))
	}
	result, err := sg.Wait()
	assert.ErrorIs(t, err, expecterr, "A correct error is returned")
	sort.Ints(expected)
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func TestKeepAllResults(t *testing.T) {
	sg := New[int](0)
	sg.KeepAllResults(true)
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args))
	expecterr := &ScatteredError{}
	for i := range args {
		args[i] = i
		if i%2 != 0 {
			expected[i] = i * i
		} else {
			expected[i] = 0
			expecterr.AddError(&cantEven{})
		}
		sg.Run(ctx, squareOdds(i))
	}
	result, err := sg.Wait()
	assert.ErrorIs(t, err, expecterr, "A correct error is returned")
	sort.Ints(expected)
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func square(i int) func() (int, error) {
	return func() (int, error) { return i * i, nil }
}

type cantEven struct{}

func (*cantEven) Error() string {
	return "I can't even"
}
func (*cantEven) Is(err error) bool {
	_, ok := err.(*cantEven)
	return ok
}

func squareOdds(i int) func() (int, error) {
	return func() (int, error) {
		if i%2 == 0 {
			return 0, &cantEven{}
		}
		return i * i, nil
	}
}

func sleepTest(i int) func() (int, error) {
	return func() (int, error) {
		time.Sleep(time.Second / 2)
		return i, nil
	}
}

func TestWithSemaphore(t *testing.T) {
	sg := New[int](1)
	ctx := context.Background()
	s := semaphore.NewWeighted(1)
	attempts := 3
	expected := make([]int, attempts)
	for i := range expected {
		expected[i] = 1
		sg.Run(ctx, semTester(s))
	}
	result, err := sg.Wait()
	assert.Nil(t, err)
	sort.Ints(result)
	assert.Equal(t, expected, result, "No concurrent runs detected")
}

func semTester(s *semaphore.Weighted) func() (int, error) {
	return func() (int, error) {

		if s.TryAcquire(1) {
			// We grabbed the semaphore, sleep and return
			defer s.Release(1)
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		}
		// we failed to grab the semaphore, so we're running in parallel with another one!
		return 0, fmt.Errorf("Failed to aquire semaphore")
	}
}

func TestCanceledContext(t *testing.T) {
	sg := New[int](3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	<-ctx.Done()
	for i := 0; i < 1000; i++ {
		sg.Run(ctx, square(i))
	}
	results, err := sg.Wait()
	assert.NotEqual(t, 1000, len(results), "No results returned")
	assert.Equal(t, "context canceled", err.(*ScatteredError).Errors[0].Error())
}

func TestSetParallel(t *testing.T) {
	start := time.Now()
	sg := New[int](0)
	sg.SetParallel(1)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		sg.Run(ctx, sleepTest(i))
	}
	time.Sleep(600 * time.Millisecond)
	sg.SetParallel(50)
	results, err := sg.Wait()
	end := time.Now()
	assert.Nil(t, err)
	assert.Equal(t, 100, len(results), "We have 100 results")
	assert.Equal(t, end.Sub(start).Truncate(100*time.Millisecond), 1600*time.Millisecond, "We ran in 1.6 seconds")
}
