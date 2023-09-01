package main

import (
	"main/large_csv_generator"
)

const (
	numRowsTotal   = 10000
	numGoroutines  = 10
	numRowsPerFile = numRowsTotal / numGoroutines
)

func main() {
	large_csv_generator.GenerateLargeCSVParallel(numRowsPerFile, numGoroutines)
	large_csv_generator.GenerateLargeCSV(numRowsTotal)
}
