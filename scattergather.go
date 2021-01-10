// A generic scatter/gather implementation to distribute work among many goroutines
package scattergather

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"golang.org/x/sync/semaphore"
)

type ScatteredFunction func(context.Context, ...interface{})(interface{}, error)

type ScatterGather struct {
	waitGroup       *sync.WaitGroup
	results         []interface{}
	errors          *ScatteredError
	resultChan      chan scatterResult
	doneChan        chan interface{}
	initOnce        sync.Once
	gatherOnce      sync.Once
	semaphore       *semaphore.Weighted
}

type scatterResult struct {
	val interface{}
	err error
}

// Create a new ScatterGather object that will run at most parallel tasks in
// parallel. When parallel is 0, the maximum is set to GOMAXPROCS.
func New(parallel int64) *ScatterGather {
	sg := &ScatterGather{}
	sg.init(parallel)
	return sg
}

func (sg *ScatterGather) init(parallel int64) {
	sg.initOnce.Do(func() {
		if parallel == 0 {
			parallel = int64(runtime.GOMAXPROCS(0))
		}
		sg.waitGroup = &sync.WaitGroup{}
		sg.results = make([]interface{}, 0)
		sg.errors = &ScatteredError{}
		sg.errors.Errors = make([]error, 0)
		sg.resultChan = make(chan scatterResult, 10)
		sg.doneChan = make(chan interface{})
		sg.semaphore = semaphore.NewWeighted(parallel)
	})
}

func (sg *ScatterGather) gather() {
	sg.gatherOnce.Do(func(){
		go sg.gatherer()
	})
}

func (sg *ScatterGather) gatherer() {
	for res := range sg.resultChan {
		if res.val != nil {
			sg.results = append(sg.results, res.val)
		}
		if res.err != nil {
			sg.errors.AddError(res.err)
		}
	}
	close(sg.doneChan)
}

// Add a piece of work to be run. This will call the callable in a separate
// goroutine and pass the context and arguments. The result and error returned
// by this function will be collected and returned from Wait()
func (sg *ScatterGather) Run(callable ScatteredFunction, ctx context.Context, args ...interface{}) {
	sg.init(0)
	sg.gather()
	sg.waitGroup.Add(1)
	go func() {
		defer sg.waitGroup.Done()
		if err := sg.semaphore.Acquire(ctx, 1); err != nil {
			sg.resultChan <-scatterResult{val: nil, err: err}
			return
		}
		defer sg.semaphore.Release(1)
		ret, err := callable(ctx, args...)
		sg.resultChan <-scatterResult{val: ret, err: err}
	}()
}

// Wait for all subtasks to return. The return value is a list of values
// returned from all subtasks, excluding any nil that was returned. The
// returned error is either `nil` to indicate no subtask returned an error or a
// *ScatteredError containing all errors returned by subtasks.
func (sg *ScatterGather) Wait() ([]interface{}, error) {
	sg.waitGroup.Wait()
	close(sg.resultChan)
	<-sg.doneChan
	if !sg.errors.HasErrors() {
		return sg.results, nil
	}
	return sg.results, sg.errors
}

// An error type that represents a collection of errors
type ScatteredError struct {
	Errors []error
}

// Whether any errors have been added to this object
func (e *ScatteredError) HasErrors() bool {
	return e != nil && e.Errors != nil && len(e.Errors) > 0
}

// Add an error to the collection
func (e *ScatteredError) AddError(err error) {
	if e.Errors == nil {
		e.Errors = []error{err}
	} else {
		e.Errors = append(e.Errors, err)
	}
}

// Returns a string containing all errors, separated by newlines
func (e *ScatteredError) Error() string {
	if e == nil {
		return "(nil error)"
	}
	if e.Errors == nil || len(e.Errors) == 0 {
		return "(empty scattered error)"
	}
	errstr := e.Errors[0].Error()
	for _, err := range(e.Errors[1:]) {
		errstr += "\n" + err.Error()
	}
	return errstr
}

// ScatteredErrors are identical iff the errors in their collections are identical
func (e *ScatteredError) Is(target error) bool {
	t, ok := target.(*ScatteredError)
	if !ok || len(e.Errors) != len(t.Errors) {
		return false
	}
	for i, err := range t.Errors {
		if !errors.Is(err, e.Errors[i]) {
			return false
		}
	}
	return true
}
