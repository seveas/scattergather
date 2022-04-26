package scattergather_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/seveas/scattergather"
)

// Square a bunch of numbers in parallel
func ExampleScatterGather() {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ctx := context.Background()
	sg := scattergather.New[int](int64(len(input)))
	// Start all the workers
	for _, i := range input {
		sg.Run(ctx, square(i))
	}
	// And wait for them to finish
	output, err := sg.Wait()
	if err != nil {
		panic(err)
	}
	// Values may be returned in any order, so sort them for correct output
	sort.Ints(output)
	fmt.Printf("The squares of %v are %v\n", input, output)
}

func square(i int) func() (int, error) {
	return func() (int, error) { return i * i, nil }
}
