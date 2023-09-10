package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"
)

const (
	numRowsTotal   = 10000000
	numGoroutines  = 10
	numRowsPerFile = numRowsTotal / numGoroutines
)

func main() {
	// GenerateLargeCSVParallel(numRowsPerFile, numGoroutines, "test_data")
	GenerateLargeCSVParallelToOneFile(numRowsPerFile, numGoroutines, "test_data")
}

// GenerateLargeCSV generates a CSV file with numRows rows
func GenerateLargeCSV(numRows int, fileName string) {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	file, err := os.Create(fmt.Sprintf("data/%s.csv", fileName))
	if err != nil {
		panic(err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("error occurred in file.Close() err: %+v", err)
		}
	}()

	writer := csv.NewWriter(file)
	for i := 0; i < numRows; i++ {
		row := generateFakeRow()
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
	writer.Flush()
	if writer.Error() != nil {
		panic(err)
	}
}

func generateFakeRow() []string {
	// 1 year ago
	startDate := time.Now().AddDate(-1, 0, 0)
	endDate := time.Now()
	return []string{
		gofakeit.UUID(),
		fmt.Sprintf("%d", gofakeit.Uint8()),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
	}
}

// Parallelize CSV generation
func GenerateLargeCSVParallel(numRows, numGoroutines int, fileName string) {
	var wg sync.WaitGroup
	// Add numGoroutines to the WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		// Call GenerateLargeCSV in a goroutine for numGoroutines times
		go func(wg *sync.WaitGroup, i int) {
			fileName := fmt.Sprintf("%s_%d", fileName, i)
			GenerateLargeCSV(numRows, fileName)
			// Decrement the WaitGroup counter after each goroutine finishes
			defer wg.Done()
		}(&wg, i)
	}
	// Wait for all goroutines to finish
	wg.Wait()
	// fmt.Printf("Done GenerateLargeCSVParallel")
}

// Parallelize CSV generation
func GenerateLargeCSVParallelToOneFile(numRows, numGoroutines int, fileName string) {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	file, err := os.Create(fmt.Sprintf("data/%s.csv", fileName))
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf("error occurred in file.Close() err: %+v", err)
		}
	}()

	var wg sync.WaitGroup
	// Add numGoroutines to the WaitGroup
	wg.Add(numGoroutines)

	writer, err := NewCSVWriter(file)
	if err != nil {
		panic(err)
	}
	for i := 0; i < numGoroutines; i++ {
		// Call GenerateLargeCSV in a goroutine for numGoroutines times
		go func(wg *sync.WaitGroup, i int, writer *CsvWriter) {
			GenerateLargeCSVWithLock(numRows, writer)
			// Decrement the WaitGroup counter after each goroutine finishes
			defer wg.Done()
		}(&wg, i, writer)
	}
	// Wait for all goroutines to finish
	wg.Wait()
	// fmt.Printf("Done GenerateLargeCSVParallelToOneFile")
}

func GenerateLargeCSVWithLock(numRows int, writer *CsvWriter) {
	for i := 0; i < numRows; i++ {
		row := generateFakeRow()
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
	writer.Flush()
}

// thread safe csv writer
type CsvWriter struct {
	mutex     *sync.Mutex
	csvWriter *csv.Writer
}

func NewCSVWriter(file io.Writer) (*CsvWriter, error) {
	w := csv.NewWriter(file)
	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
}

// lock and write
func (w *CsvWriter) Write(row []string) error {
	w.mutex.Lock()
	err := w.csvWriter.Write(row)
	if err != nil {
		return fmt.Errorf("error occurred in csvWriter.Write: err %+v", err)
	}
	w.mutex.Unlock()
	return nil
}

// lock and flush
func (w *CsvWriter) Flush() {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}

func CleanUp() {
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal(err)
	}
}
