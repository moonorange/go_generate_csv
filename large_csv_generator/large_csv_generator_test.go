package large_csv_generator

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"testing"
)

const (
	totalNumRows = 10000000
	fileName     = "test_data"
)

func BenchmarkGenerateLargeCSVParallel(b *testing.B) {
	GenerateLargeCSVParallel(totalNumRows/10, 10, fileName)
	defer CleanUp()

}

func BenchmarkGenerateLargeCSV(b *testing.B) {
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
	GenerateLargeCSV(totalNumRows, writer)
	defer CleanUp()
}

func BenchmarkGenerateLargeCSVParallelToOneFile(b *testing.B) {
	GenerateLargeCSVParallelToOneFile(totalNumRows/10, 10, fileName)
	defer CleanUp()
}

func CleanUp() {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}
}
