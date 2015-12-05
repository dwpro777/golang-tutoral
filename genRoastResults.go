package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

func main() {

	//open up sqlite database file for reading
	fmt.Println("Starting app")
	dbName := "./roast.db3"
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//timer := time.Now()
	//QueryAndStore(db, false)
	//fmt.Println("Done without threading. took ", time.Since(timer))

	timer := time.Now()
	QueryAndStore(db, true)
	fmt.Println("Done with threading. took ", time.Since(timer))
}

func QueryAndStore(db *sql.DB, useThreading bool) {

	//Initialize waitgroup, this will keep track of what all is processing
	var wg sync.WaitGroup

	//create output directories if they do not exist
	err := os.Mkdir("csvDir", 0775)
	if err != nil {
		if !os.IsExist(err) {
			log.Fatal(err)
		}
	}
	err = os.Mkdir("images", 0775)
	if err != nil {
		if !os.IsExist(err) {
			log.Fatal(err)
		}
	}

	//select the main identifiers for processing
	roastRows, err := db.Query(`select distinct Id from RoastLog;`)
	if err != nil {
		log.Fatal(err)
	}

	//iterate through the selected rows, for each one, write the output files
	defer roastRows.Close()
	for roastRows.Next() {
		var rowId int
		roastRows.Scan(&rowId)
		//fmt.Println("getting info for row ", rowId)
		wg.Add(1)
		if useThreading {
			go writeFiles(db, rowId, &wg)
		} else {
			writeFiles(db, rowId, &wg)
		}
	}
	wg.Wait()
}

func writeFiles(db *sql.DB, rowId int, wg *sync.WaitGroup) {
	//select out the temperatures and times from the database for each roast
	queryString := `
	select RoasterTempInDegrees, round((packetReceived - RoastStart )/10000000.00/60.00,2) as rTime from roastLogPacket join RoastLog on RoastLogPacket.RoastLogId = RoastLog.id where roastlogid=%s and RoasterTempInDegrees > 0 order by rTime asc;`
	rows, err := db.Query(fmt.Sprintf(queryString, strconv.Itoa(rowId)))
	if err != nil {
		log.Fatal(err)
	}

	//output a csv file for the rscript to parse and use
	csvFileName := fmt.Sprintf("csvDir/roastOutput%d.csv", rowId)
	csvfile, err := os.Create(csvFileName)
	if err != nil {
		//fmt.Println("Error:", err)
		return
	}
	defer csvfile.Close()

	writer := csv.NewWriter(csvfile)
	writer.Comma = '\t'
	outputArr := [][]string{}

	//output the header row by itself then clear out the row
	rowToWrite := []string{"RoasterTempInDegrees", "rTime"}
	outputArr = append(outputArr, rowToWrite)
	rowToWrite = []string{}
	/*iterate through the returned rows, storing the temp and time into variables that are then
	put into an array for writing to the output csv file */
	for rows.Next() {
		var rTemp int
		var rTime float64
		rows.Scan(&rTemp, &rTime)
		rowToWrite := []string{}
		rowToWrite = append(rowToWrite, strconv.Itoa(rTemp))
		rowToWrite = append(rowToWrite, strconv.FormatFloat(rTime, 'f', 2, 64))
		outputArr = append(outputArr, rowToWrite)
	}
	fmt.Printf("finished querying and building for id %d, now writing to file\n", rowId)
	writer.WriteAll(outputArr)
	writer.Flush()

	//create command for calling the R script for the given ID
	cmd := exec.Command("./getRoastPng.R", strconv.Itoa(rowId))
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Wait()
	fmt.Printf("finished running R script for rowID: %d\n", rowId)

	//let the waitgroup know that this work is finished.
	wg.Done()

}
