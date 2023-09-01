package main

import (
	"main/large_csv_generator"
)

const (
	numRowsTotal   = 100000
	numGoroutines  = 10
	numRowsPerFile = numRowsTotal / numGoroutines
)

func main() {
	large_csv_generator.GenerateLargeCSVParallel(numRowsPerFile, numGoroutines, "test_data")
	// large_csv_generator.GenerateLargeCSV(numRowsTotal, "test_data")
}
