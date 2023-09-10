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
	fileName     = "test_data"
	fileName2    = "test_data2"
	totalNumRows = 1000000000
)

var testCases = []struct {
	totalNumRows  int
	numGoroutines int
}{
	{totalNumRows: totalNumRows, numGoroutines: 5},
	{totalNumRows: totalNumRows, numGoroutines: 10},
	{totalNumRows: totalNumRows, numGoroutines: 15},
	{totalNumRows: totalNumRows, numGoroutines: 20},
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
	b.Run(fmt.Sprintf("totalNumRows=%d", totalNumRows), func(b *testing.B) {
		GenerateLargeCSV(totalNumRows, writer)
	})
}

func BenchmarkGenerateLargeCSVParallelToOneFile(b *testing.B) {
	SetUp()
	defer CleanUp()
	// for _, tc := range testCases {
	// 	b.Run(fmt.Sprintf("totalNumRows=%d,numGoroutines=%d", tc.totalNumRows, tc.numGoroutines), func(b *testing.B) {
	GenerateLargeCSVParallelToOneFile(totalNumRows/10, 10, fileName2)
	// })
	// }
}

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
