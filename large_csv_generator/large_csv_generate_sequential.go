package large_csv_generator

import (
	"encoding/csv"
	"os"

	"github.com/go-errors/errors"
)

func GenerateLargeCSV(numRows int) {
	err := os.Mkdir("data", 0777)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			panic(err)
		}
	}
	file, err := os.Create("data/fak_data.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for j := 0; j < numRows; j++ {
		row := generateFakeRow()
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
	writer.Flush()
	print("Done Sequential")
}
