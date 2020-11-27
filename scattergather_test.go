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
	sg := New(0)
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args))
	for i, _ := range args {
		args[i] = i
		expected[i] = i * i
		sg.Run(square, ctx, i)
	}
	untypedResult, err := sg.Wait()
	assert.Nil(t, err, "No error is returned")
	result:= make([]int, len(untypedResult))
	for i, v := range(untypedResult) {
		result[i] = v.(int)
	}
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func TestWithErrors(t *testing.T) {
	sg := New(0)
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args))
	expecterr := &ScatteredError{}
	for i, _ := range args {
		args[i] = i
		if i % 2 != 0 {
			expected[i] = i * i
		} else {
			expecterr.AddError(&cantEven{})
		}
		sg.Run(squareOdds, ctx, i)
	}
	sort.Ints(expected)
	untypedResult, err := sg.Wait()
	assert.ErrorIs(t, err, expecterr, "A correct error is returned")
	result:= make([]int, len(untypedResult))
	for i, v := range(untypedResult) {
		result[i] = v.(int)
	}
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func TestWithErrorsAndPartialResults(t *testing.T) {
	sg := New(0)
	ctx := context.Background()
	args := make([]int, cap(sg.resultChan)+10)
	expected := make([]int, cap(args)/2)
	expecterr := &ScatteredError{}
	for i, _ := range args {
		args[i] = i
		if i % 2 != 0 {
			expected[i/2] = i * i
		} else {
			expecterr.AddError(&cantEven{})
		}
		sg.Run(squareOddsOrNil, ctx, i)
	}
	sort.Ints(expected)
	untypedResult, err := sg.Wait()
	assert.ErrorIs(t, err, expecterr, "A correct error is returned")
	result:= make([]int, len(untypedResult))
	for i, v := range(untypedResult) {
		result[i] = v.(int)
	}
	sort.Ints(result)
	assert.Equal(t, expected, result, "We correctly square an array of integers")
}

func square(ctx context.Context, args ...interface{}) (interface{}, error) {
	v := args[0].(int)
	return v * v, nil
}

type cantEven struct {}
func (*cantEven) Error() string {
	return "I can't even"
}
func (*cantEven) Is(err error) bool {
	_, ok := err.(*cantEven)
	return ok
}

func squareOdds(ctx context.Context, args ...interface{}) (interface{}, error) {
	v := args[0].(int)
	if v % 2 == 0 {
		return 0, &cantEven{}
	} else {
		return v * v, nil
	}
}

func squareOddsOrNil(ctx context.Context, args ...interface{}) (interface{}, error) {
	v := args[0].(int)
	if v % 2 == 0 {
		return nil, &cantEven{}
	} else {
		return v * v, nil
	}
}

func TestWithSemaphore(t *testing.T) {
	sg := New(1)
	ctx := context.Background()
	s := semaphore.NewWeighted(1)
	attempts := 3
	expected := make([]int, attempts)
	for i, _ := range(expected) {
		expected[i] = 1
		sg.Run(semTester, ctx, s)
	}
	untypedResult, err := sg.Wait()
	assert.Nil(t, err)
	result:= make([]int, len(untypedResult))
	for i, v := range(untypedResult) {
		result[i] = v.(int)
	}
	sort.Ints(result)
	assert.Equal(t, expected, result, "No concurrent runs detected")
}

func semTester(ctx context.Context, args ...interface{}) (interface{}, error) {
	s := args[0].(*semaphore.Weighted)

	if s.TryAcquire(1) {
		// We grabbed the semaphore, sleep and return
		defer s.Release(1)
		time.Sleep(50*time.Millisecond)
		return 1, nil
	} else {
		// we failed to grab the semaphore, so we're running in parallel with another one!
		return nil, fmt.Errorf("Failed to aquire semaphore")
	}
}
