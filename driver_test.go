package gojdbc

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

type Test struct {
	Id      int64
	Title   string
	Age     int64
	Created time.Time
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

	_, err = db.Exec("create table test(Id int auto_increment primary key, Title varchar(255), Age int, Created datetime)")
	fatalErr(err)

	// Parallel inserts
	// TODO: This is very slow currently
	testTime := time.Now().Round(time.Second)
	tx, err := db.Begin()
	fatalErr(err)
	stmt, err := tx.Prepare("insert into test(Title,Age,Created) values(?,?,?)")
	//stmt = tx.Stmt(stmt)
	fatalErr(err)
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			r, err := stmt.Exec(fmt.Sprintf("The %d", i), i, testTime)
			fatalErr(err)
			_, err = r.RowsAffected()
			fatalErr(err)
		}(i)
	}
	wg.Wait()
	fatalErr(tx.Commit())

	// Select rows
	rows, err := db.Query("select * from test")
	fatalErr(err)
	defer rows.Close()

	i := 0
	for rows.Next() {
		i = i + 1
		r := Test{}
		if e := rows.Scan(&r.Id, &r.Title, &r.Age, &r.Created); e != nil {
			t.Fatal(e)
		}
		expectedTitle := fmt.Sprintf("The %d", r.Age)
		switch {
		case r.Id == 0:
			t.Fatalf("Invalid Id: %+v", r)
		case r.Title != expectedTitle:
			t.Fatalf("Expected Title %s but got %s", expectedTitle, r.Title)
		case !r.Created.Equal(testTime):
			t.Fatalf("Expected time %v but got %v", testTime, r.Created)

		}
	}
	if i < 100 {
		t.Fatalf("Expected 100 but got %d.", i)
	}

}
