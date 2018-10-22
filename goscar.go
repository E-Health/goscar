package goscar

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"time"
	"flag"
)

//This is how you declare a global variable
// var csvMap, csvMapValid []map[string]string
var recordCount int
var sshHost, sshUser, sshPass, dbUser, dbPass, dbHost, dbName, dateFrom, dateTo, filePtr *string
var sshPort, fid *int
var includeAll *bool

func init(){
	// Commandline flags
	sshHost = flag.String("sshhost", "", "The SSH host")
	sshPort = flag.Int("sshport", 22, "The port number")
	sshUser = flag.String("sshuser", "ssh-user", "ssh user")
	sshPass = flag.String("sshpass", "ssh-pass", "SSH Password")
	dbUser = flag.String("dbuser", "dbuser", "The db user")
	dbPass = flag.String("dbpass", "dbpass", "The db password")
	dbHost = flag.String("dbhost", "localhost:3306", "The db host")
	dbName = flag.String("dbname", "oscar", "The database name")
	dateFrom = flag.String("datefrom", "oscar", "The start date")
	dateTo = flag.String("dateto", "oscar", "The end date")
	fid = flag.Int("fid", 1, "The eform ID")
	filePtr = flag.String("file", "", "The csv file to process")
	includeAll = flag.Bool("include", false, "Include all records")
	flag.Parse()
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
			mysqlRows.Scan(&id, &fdid, &fid, &demographic_no, &var_name, &var_value)
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

func FindDuplicates(csvMap []map[string]string) ([]map[string]string, int){
	var latest bool
	var included bool
	var demographicNo []string
	var csvMapValid []map[string]string
	for _, v := range csvMap {
		latest = false
		included = true
		for k2, v2 := range v {
			if k2 == "eft_latest" && v2 == "1" {
				latest = true
			}
			if k2 == "dateCreated" {
				dateCreated, _ := time.Parse("2006-01-02", v2)
				_dateFrom, _ := time.Parse("2006-01-02", *dateFrom)
				_dateTo, _ := time.Parse("2006-01-02", *dateTo)
				if len(*dateFrom) > 0 && len(*dateTo) > 0 && !InTimeSpan(_dateFrom, _dateTo, dateCreated) {
					included = false
				}
			}
			if k2 == "demographic_no" {
				if !IsMember(v2, demographicNo){
					demographicNo = append(demographicNo, v2)
					latest = true
				}
			}
			if *includeAll {
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