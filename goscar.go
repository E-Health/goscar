package goscar

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	stats "github.com/montanaflynn/stats"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

//This is how you declare a global variable
// var csvMap, csvMapValid []map[string]string

func init() {
	// Commandline flags removed
}

// CSVToMap takes a reader and returns an array of dictionaries, using the header row as the keys
func CSVToMap(reader io.Reader) []map[string]string {
	r := csv.NewReader(reader)
	rows := []map[string]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows
}

func MysqlToMap(mysqlRows *sql.Rows) []map[string]string {
	rows := []map[string]string{}
	row := map[string]string{}
	var header []string
	var prevFdid int64 = 0
	if mysqlRows != nil {
		for mysqlRows.Next() {
			var id int64
			var fdid int64
			var fid int64
			var demographic_no int64
			var var_name string
			var var_value string
			// Ignores errors
			_ = mysqlRows.Scan(&id, &fdid, &fid, &demographic_no, &var_name, &var_value)
			if !IsMember(var_name, header) {
				header = append(header, var_name)
			}
			row[var_name] = var_value
			if prevFdid == 0 {
				prevFdid = fdid
			}
			if fdid != prevFdid {
				rows = append(rows, row)
				prevFdid = fdid
			}
		}
	} else {
		os.Exit(2)
	}
	return rows
}

func IsMember(s string, a []string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}

func InTimeSpan(start, end, check time.Time) bool {
	return check.After(start) && check.Before(end)
}

// Takes the slice, fromDate like 2016-01-02 toDate and includeAll bool. Returns the selected slice and record count)
func FindDuplicates(csvMap []map[string]string, dateFrom string, dateTo string, includeAll bool) ([]map[string]string, int) {
	var latest bool
	var included bool
	var demographicNo []string
	var csvMapValid []map[string]string
	recordCount := 0
	for _, v := range csvMap {
		latest = false
		included = true
		for k2, v2 := range v {
			if k2 == "eft_latest" && v2 == "1" {
				latest = true
			}
			if k2 == "dateCreated" {
				dateCreated, _ := time.Parse("2006-01-02", v2)
				_dateFrom, _ := time.Parse("2006-01-02", dateFrom)
				_dateTo, _ := time.Parse("2006-01-02", dateTo)
				if len(dateFrom) > 0 && len(dateTo) > 0 && !InTimeSpan(_dateFrom, _dateTo, dateCreated) {
					included = false
				}
			}
			if k2 == "demographic_no" {
				if !IsMember(v2, demographicNo) {
					demographicNo = append(demographicNo, v2)
					latest = true
				}
			}
			if includeAll {
				latest = true
			}
		}
		if latest && !included {
			csvMapValid = append(csvMapValid, v)
			recordCount++
		}
	}
	return csvMapValid, recordCount
}

// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func getStats(key string, recordCount int, csvMapValid []map[string]string) map[string]float64 {
	varType := "string"
	counter := make(map[string]int)
	varNum := []float64{}
	toReturn := make(map[string]float64)
	for _, record := range csvMapValid {
		if n, err := strconv.ParseFloat(record[key], 64); err == nil {
			varNum = append(varNum, n)
			varType = "num"
		} else {
			counter[record[key]]++

		}
		// https://stackoverflow.com/questions/44417913/go-count-distinct-values-in-array-performance-tips
	}
	distinctStrings := make([]string, len(counter))
	i := 0
	for k := range counter {
		distinctStrings[i] = k
		i++
	}
	for _, s := range distinctStrings {
		toReturn["count"] = float64(counter[s])
		toReturn["percent"] = float64(counter[s] * 100 / recordCount)
		toReturn["num"] = 0
	}
	if varType == "num" {
		toReturn["num"] = 1
		a, _ := stats.Sum(varNum)
		toReturn["sum"] = a
		a, _ = stats.Min(varNum)
		toReturn["min"] = a
		a, _ = stats.Max(varNum)
		toReturn["max"] = a
		a, _ = stats.Mean(varNum)
		toReturn["mean"] = a
		a, _ = stats.Median(varNum)
		toReturn["median"] = a
		a, _ = stats.StandardDeviation(varNum)
		toReturn["stddev"] = a

	}
	return toReturn
}
