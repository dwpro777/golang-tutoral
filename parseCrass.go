package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func main() {

	//stick into database
	dbName := "./crass.db3"
	os.Remove(dbName)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Creating the database")
	createTable(db)
	fmt.Println("parsing out the files...")

	ReadFiles(db)
	fmt.Println("querying the data and writing output...")
	QueryAndStore(db)
	fmt.Println("Done")

}

func createTable(db *sql.DB) {
	sqlStmt := `
		create table sample (pos int, val text, sampName text);
		delete from sample;	
		create index indx_samp on sample(pos,sampName);
		create table masterSample(pos int, val text);
		create index indx_master on masterSample(pos);
		delete from masterSample;	
		create table sampleName(name text);
		create index indx_sampName on sampName(name); 
		delete from sampleName;

			`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
}

func ReadFiles(db *sql.DB) {
	var wg sync.WaitGroup
	tx, err := db.Begin()

	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare("insert into sample(pos, val, sampName) values(?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	defer stmt.Close()

	//read all csv files in dir
	files, _ := ioutil.ReadDir("./")
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), "pos") {
			continue
		}
		//fmt.Println(f.Name())
		wg.Add(1)
		go parseFileFnc(f.Name(), stmt, &wg)

	}

	wg.Wait()
	tx.Commit()
	//select distinct samples into sample table
	sqlInsStmt := `insert into sampleName select distinct sampName from sample;`
	_, err = db.Exec(sqlInsStmt)
	if err != nil {
		log.Fatal(err)
		return
	}

	//read in master table
	txMas, err := db.Begin()

	if err != nil {
		log.Fatal(err)
	}
	stmtInsMaster, err := txMas.Prepare("insert into masterSample(pos, val) values(?, ?)")

	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open("crass.gen.var")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)

	fmt.Println("parsing the master file")
	reader.Comma = '\t'
	for {
		//parse out into needed fields
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(record[0], record[1])
		_, err = stmtInsMaster.Exec(record[0], record[1])
		if err != nil {
			log.Fatal(err)
		}
	}
	txMas.Commit()

}

func parseFileFnc(fName string, stmt *sql.Stmt, wg *sync.WaitGroup) {

	defer wg.Done()
	fmt.Println("parsing file", fName)

	file, err := os.Open(fName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)

	reader.Comma = '\t'
	for {
		//parse out into needed fields
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(record[0], record[1])
		_, err = stmt.Exec(record[0], record[2], fName)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func QueryAndStore(db *sql.DB) {

	rows, err := db.Query(`
select ms.pos, ms.val, sn.name, s.val
from mastersample ms, sampleName sn
left join sample s on s.pos = ms.pos and sn.name = s.sampName
order by ms.pos, sn.name;
`)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	csvfile, err := os.Create("output.csv")
	if err != nil {
		//fmt.Println("Error:", err)
		return
	}
	defer csvfile.Close()

	writer := csv.NewWriter(csvfile)
	writer.Comma = '\t'
	priorPos := 0
	headerRow := []string{""}
	var rowToWrite []string
	outputArr := [][]string{}
	hasWrittenHeader := false
	for rows.Next() {
		var pos int
		var mVal, name string
		var sVal sql.NullString
		rows.Scan(&pos, &mVal, &name, &sVal)
		//capture all the header rows
		if hasWrittenHeader == false && (pos == priorPos || priorPos == 0) {
			reItem := regexp.MustCompile(".pos$")
			headerRow = append(headerRow, reItem.ReplaceAllString(name, ""))
		}
		if pos != priorPos {
			if priorPos != 0 {
				if hasWrittenHeader == false {
					outputArr = append(outputArr, headerRow)
					hasWrittenHeader = true
				}
				outputArr = append(outputArr, rowToWrite)
			}
			priorPos = pos
			posStr := strconv.Itoa(pos)
			rowToWrite = []string{posStr}
		}
		if sVal.Valid {
			rowToWrite = append(rowToWrite, sVal.String)
		} else {
			rowToWrite = append(rowToWrite, mVal)
		}
	}
	if rowToWrite != nil {
		outputArr = append(outputArr, rowToWrite)
	}
	fmt.Println("finished querrying and building, now writing to file")
	writer.WriteAll(outputArr)
	writer.Flush()

}
