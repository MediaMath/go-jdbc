package gojdbc

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

type Test struct {
	Id    int64
	Title string
}

func TestJDBC(t *testing.T) {
	fatalErr := func(e error) {
		if e != nil {
			t.Fatal(e)
		}
	}
	db, err := sql.Open("jdbc", "tcp://localhost:7777/")
	fatalErr(err)
	defer db.Close()

	_, err = db.Exec("drop table if exists test;")
	fatalErr(err)

	_, err = db.Exec("create table test(Id int auto_increment primary key, Title varchar(255))")
	fatalErr(err)

	// Parallel inserts
	// TODO: This is very slow currently
	tx, err := db.Begin()
	fatalErr(err)
	stmt, err := db.Prepare("insert into test(Title) values(?)")
	stmt = tx.Stmt(stmt)
	fatalErr(err)
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			r, err := stmt.Exec(fmt.Sprintf("The %d", i))
			fatalErr(err)
			if a, err := r.RowsAffected(); a != 1 {
				t.Fatal("Expected 1, got %d", a)
			} else {
				fatalErr(err)
			}
		}(i)
	}
	wg.Wait()
	fatalErr(tx.Commit())

	// Select rows
	rows, err := db.Query("select * from test")
	fatalErr(err)
	defer rows.Close()
	for rows.Next() {
		record := Test{}
		fatalErr(rows.Scan(&record.Id, &record.Title))
	}
}
