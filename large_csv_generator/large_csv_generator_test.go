package large_csv_generator

import (
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
)

const (
	fileName  = "test_data"
	fileName2 = "test_data2"
)

var testCases = []struct {
	totalNumRows  int
	numGoroutines int
}{
	{totalNumRows: 100000000, numGoroutines: 5},
	{totalNumRows: 100000000, numGoroutines: 10},
	{totalNumRows: 100000000, numGoroutines: 15},
	{totalNumRows: 100000000, numGoroutines: 20},
}

func BenchmarkGenerateLargeCSV(b *testing.B) {
	SetUp()
	defer CleanUp()

	file, err := os.Create(fmt.Sprintf("data/%s.csv", fileName))
	if err != nil {
		panic(err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}()

	writer := csv.NewWriter(file)
	b.Run(fmt.Sprintf("totalNumRows=%d", 100000000), func(b *testing.B) {
		GenerateLargeCSV(100000000, writer)
	})
	for _, tc := range testCases {
		b.Run(fmt.Sprintf("totalNumRows=%d,numGoroutines=%d", tc.totalNumRows, tc.numGoroutines), func(b *testing.B) {
			GenerateLargeCSVParallelToOneFile(tc.totalNumRows/tc.numGoroutines, tc.numGoroutines, fileName2)
		})
	}
}

// func BenchmarkGenerateLargeCSVParallelToOneFile(b *testing.B) {
// 	SetUp()
// 	for _, tc := range testCases {
// 		b.Run(fmt.Sprintf("totalNumRows=%d,numGoroutines=%d", tc.totalNumRows, tc.numGoroutines), func(b *testing.B) {
// 			GenerateLargeCSVParallelToOneFile(tc.totalNumRows/tc.numGoroutines, tc.numGoroutines, fileName)
// 		})
// 	}
// 	defer CleanUp()
// }

func SetUp() {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			log.Fatal(err)
		}
	}
}

func CleanUp() {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}
}
