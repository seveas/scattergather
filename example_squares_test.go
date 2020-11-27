package scattergather_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/seveas/scattergather"
)

// Square a bunch of numbers in parallel
func ExampleScatterGather() {
	input := []int{1,2,3,4,5,6,7,8,9,10}
	ctx := context.Background()
	sg := scattergather.New(int64(len(input)))
	// Start all the workers
	for _, i := range(input) {
		sg.Run(square, ctx, i)
	}
	// And wait for them to finish
	untypedOutput, err := sg.Wait()
	if err != nil {
		panic(err)
	}
	// Wait returns a list of interface{} values, so we must use type assertions
	output := make([]int, len(untypedOutput))
	for i, out := range(untypedOutput) {
		output[i] = out.(int)
	}
	// Values may be returned in any order, so sort them for correct output
	sort.Ints(output)
	fmt.Printf("The squares of %v are %v\n", input, output)
}

func square(ctx context.Context, args ...interface{}) (interface{}, error) {
	v := args[0].(int)
	return v * v, nil
}
