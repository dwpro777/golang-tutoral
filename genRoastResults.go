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

	//stick into database
	fmt.Println("Start")
	dbName := "./roast.db3"
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//timer := time.Now()
	//QueryAndStore(db, false)
	//fmt.Println("Done without threading. took ", time.Since(timer))

	timer = time.Now()
	QueryAndStore(db, true)
	fmt.Println("Done with threading. took ", time.Since(timer))
}

func QueryAndStore(db *sql.DB, useThreading bool) {

	var wg sync.WaitGroup
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

	roastRows, err := db.Query(`
		select distinct Id from RoastLog;
	`)
	if err != nil {
		log.Fatal(err)
	}

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
	queryString := `
	select RoasterTempInDegrees, round((packetReceived - RoastStart )/10000000.00/60.00,2) as rTime from roastLogPacket join RoastLog on RoastLogPacket.RoastLogId = RoastLog.id where roastlogid=%s and RoasterTempInDegrees > 0 order by rTime asc;`
	rows, err := db.Query(fmt.Sprintf(queryString, strconv.Itoa(rowId)))
	if err != nil {
		log.Fatal(err)
	}
	csvFileName := fmt.Sprintf("csvDir/roastOutput%d.csv", rowId)
	csvfile, err := os.Create(csvFileName)
	if err != nil {
		//fmt.Println("Error:", err)
		return
	}
	defer csvfile.Close()

	writer := csv.NewWriter(csvfile)
	writer.Comma = '\t'
	rowToWrite := []string{"RoasterTempInDegrees", "rTime"}
	outputArr := [][]string{}
	outputArr = append(outputArr, rowToWrite)
	rowToWrite = []string{}
	for rows.Next() {
		var rTemp int
		var rTime float64
		rows.Scan(&rTemp, &rTime)
		rowToWrite := []string{}
		rowToWrite = append(rowToWrite, strconv.Itoa(rTemp))
		rowToWrite = append(rowToWrite, strconv.FormatFloat(rTime, 'f', 2, 64))
		//capture all the header rows
		outputArr = append(outputArr, rowToWrite)
	}
	fmt.Printf("finished querying and building for id %d, now writing to file\n", rowId)
	writer.WriteAll(outputArr)
	writer.Flush()

	cmd := exec.Command("./getRoastPng.R", strconv.Itoa(rowId))
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Wait()
	fmt.Printf("finished running R script for rowID: %d\n", rowId)
	wg.Done()

}
