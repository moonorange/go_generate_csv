package large_csv_generator

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/go-errors/errors"
)

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
	defer file.Close()

	writer := csv.NewWriter(file)
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
