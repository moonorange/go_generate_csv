package large_csv_generator

import (
	"log"
	"os"
	"testing"
)

const totalNumRows = 500000

func BenchmarkGenerateLargeCSVParallel(b *testing.B) {
	GenerateLargeCSVParallel(totalNumRows/10, 10)
	defer CleanUp()

}

func BenchmarkGenerateLargeCSV(b *testing.B) {
	GenerateLargeCSV(totalNumRows)
	defer CleanUp()
}

func CleanUp() {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}
}
