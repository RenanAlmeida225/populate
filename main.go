package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

const MAX_CHUNK int = 1000 //limit of postgres

var db *sql.DB = openConn()
var saved int = 0

func openConn() *sql.DB {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=p@ssw0rd dbname=populate_db sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	return db
}

func main() {
	ch := make(chan [][]string, 2)
	go ReadCsv(ch)
	Save(ch)
	fmt.Printf("saved %d in database\n", saved)
	defer db.Close()
}

func ReadCsv(ch chan<- [][]string) {
	chunk := [][]string{}
	chunk_len := 0
	first_record := true
	last_record := false

	file, err := os.Open("movie.csv")
	if err != nil {
		log.Fatalf("fail on open file %s", err)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	for {
		record, _ := reader.Read()
		if record == nil {
			last_record = true
		}
		if !last_record && !first_record {
			record[1] = strings.Replace(record[1], "'", "''", -1)
			chunk = append(chunk, record)
			chunk_len++
		}
		if record != nil {
			first_record = false
		}
		if chunk_len == MAX_CHUNK {
			ch <- chunk
			chunk = [][]string{}
			chunk_len = 0
		} else if last_record {
			ch <- chunk
			break
		}
	}
	close(ch)
}

func buildInsert(chunk [][]string) (sql string) {
	sql = "INSERT INTO movies (id, title, genrs) VALUES "
	for i, v := range chunk {
		if i == len(chunk)-1 {
			sql += fmt.Sprintf("(%s, '%s', '%s'); ", v[0], v[1], v[2])
			break
		}
		sql += fmt.Sprintf("(%s, '%s', '%s'), ", v[0], v[1], v[2])
	}
	return
}

func Save(ch <-chan [][]string) {
	for v := range ch {
		sql := buildInsert(v)
		_, err := db.Exec(sql)
		if err != nil {
			log.Fatalf("fail on save ==> %s\n", err)
		}
		saved += len(v)
	}
}
