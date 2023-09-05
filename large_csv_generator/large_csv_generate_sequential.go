package large_csv_generator

import (
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"
)

func GenerateLargeCSV(numRows int, writer *csv.Writer) {
	for i := 0; i < numRows; i++ {
		row := generateFakeRow()
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
	writer.Flush()
}

func generateFakeRow() []string {
	startDate := time.Now().AddDate(-1, 0, 0)
	endDate := time.Now()
	return []string{
		gofakeit.UUID(),
		fmt.Sprintf("%d", gofakeit.Uint8()),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
	}
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
		return err
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
