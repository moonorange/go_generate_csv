package large_csv_generator

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/go-errors/errors"
)

// Parallelize CSV generation
func GenerateLargeCSVParallel(numRows, numGoroutines int, fileName string) {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}

	var wg sync.WaitGroup
	// Add numGoroutines to the WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		// Call GenerateLargeCSV in a goroutine for numGoroutines times
		go func(wg *sync.WaitGroup, i int) {
			fileName := fmt.Sprintf("%s_%d", fileName, i)
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
			GenerateLargeCSV(numRows, writer)
			// Decrement the WaitGroup counter after each goroutine finishes
			defer wg.Done()
		}(&wg, i)
	}
	// Wait for all goroutines to finish
	wg.Wait()
	// fmt.Printf("Done GenerateLargeCSVParallel")

	err = compressCSVFiles(fmt.Sprintf("%s.zip", fileName), numGoroutines, fileName)
	if err != nil {
		panic(err)
	}
}

func compressCSVFiles(zipFileName string, numFiles int, csvFileName string) error {
	zipFile, err := os.Create(fmt.Sprintf("data/%s", zipFileName))
	if err != nil {
		return err
	}

	defer func() {
		err := zipFile.Close()
		if err != nil {
			panic(err)
		}
	}()
	zipWriter := zip.NewWriter(zipFile)
	defer func() {
		err := zipWriter.Close()
		if err != nil {
			panic(err)
		}
	}()

	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("data/%s_%d.csv", csvFileName, i)
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer func() {
			err := file.Close()
			if err != nil {
				panic(err)
			}
		}()

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
			panic(err)
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
