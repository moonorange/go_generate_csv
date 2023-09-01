package large_csv_generator

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/go-errors/errors"
)

// 並列化してみたが、こっちの方が直列より遅い
func GenerateLargeCSVParallel(numRows, numGoroutines int) {
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(wg *sync.WaitGroup) {
			random := gofakeit.Uint32()
			err := os.Mkdir("data", 0777)
			if err != nil {
				if !errors.Is(err, os.ErrExist) {
					panic(err)
				}
			}
			file, err := os.Create(fmt.Sprintf("data/%d.csv", random))
			if err != nil {
				panic(err)
			}
			defer file.Close()

			writer := csv.NewWriter(file)
			defer wg.Done()
			for j := 0; j < numRows; j++ {
				row := generateFakeRow()
				if err := writer.Write(row); err != nil {
					panic(err)
				}
			}
			writer.Flush()
		}(&wg)
	}
	wg.Wait()
	print("Done Parallel")
	// err := compressCSVFiles("fake_purchase_transactions_data.zip", numGoroutines)
	// if err != nil {
	// 	panic(err)
	// }
	// print("Compression Done")

}

func compressCSVFiles(zipFileName string, numFiles int) error {
	zipFile, err := os.Create(zipFileName)
	if err != nil {
		return err
	}

	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("fake_purchase_transactions_data%d.csv", i)
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		header.Name = fileName
		header.Method = zip.Deflate

		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		_, err = io.Copy(writer, file)
		if err != nil {
			return err
		}
	}
	return nil
}

// thread safe csv writer
// type CsvWriter struct {
// 	mutex     *sync.Mutex
// 	csvWriter *csv.Writer
// }

// func NewCSVWriter(fileName string) (*CsvWriter, error) {
// 	csvFile, err := os.Create(fileName)
// 	if err != nil {
// 		return nil, err
// 	}
// 	w := csv.NewWriter(csvFile)
// 	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
// }

// lock and write
// func (w *CsvWriter) Write(row []string) error {
// 	w.mutex.Lock()
// 	err := w.csvWriter.Write(row)
// 	if err != nil {
// 		return err
// 	}
// 	w.mutex.Unlock()
// 	return nil
// }

// lock and flush
// func (w *CsvWriter) Flush() {
// 	w.csvWriter.Flush()
// }

func generateFakeRow() []string {
	startDate := time.Now().AddDate(-1, 0, 0)
	endDate := time.Now()
	return []string{
		gofakeit.UUID(),
		gofakeit.UUID(),
		gofakeit.UUID(),
		fmt.Sprintf("%d", gofakeit.Uint8()),
		fmt.Sprintf("%d", gofakeit.Uint8()),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
		gofakeit.DateRange(startDate, endDate).Format(time.DateTime),
	}
}
