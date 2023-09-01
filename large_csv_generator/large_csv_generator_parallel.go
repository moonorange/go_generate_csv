package large_csv_generator

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"sync"
)

// 並列化してCSVを生成する
func GenerateLargeCSVParallel(numRows, numGoroutines int, fileName string) {
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(wg *sync.WaitGroup, i int) {
			fileName := fmt.Sprintf("%s_%d", fileName, i)
			GenerateLargeCSV(numRows, fileName)
			defer wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	print("Done GenerateLargeCSVParallel")

	err := compressCSVFiles("test_data.zip", numGoroutines, fileName)
	if err != nil {
		panic(err)
	}
	print("Compression Done")

}

func compressCSVFiles(zipFileName string, numFiles int, csvFileName string) error {
	zipFile, err := os.Create(fmt.Sprintf("data/%s", zipFileName))
	if err != nil {
		return err
	}

	defer zipFile.Close()
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("data/%s_%d.csv", csvFileName, i)
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
		os.Remove(fileName)
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
