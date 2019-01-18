package main_test

import (
	"database/sql"
	"event-sourcing-demo/repo"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func dbConn() *sql.DB {
	db, err := sql.Open("postgres", "user=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

func setupDB() {
	db := dbConn()
	dbDO(db, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	dbDO(db, `
		CREATE TABLE events (
			id SERIAL PRIMARY KEY,
			widget_id uuid NOT NULL,
			version INT NOT NULL,
			value TEXT NOT NULL
		)`)
	dbDO(db, `
		CREATE TABLE views (
			id SERIAL PRIMARY KEY,
			widget_id uuid DEFAULT uuid_generate_v4 (),
			version INT NOT NULL,
			value TEXT NOT NULL
		)`)
	dbDO(db, `CREATE UNIQUE INDEX events_widget_id_version on events (widget_id, version)`)
}

func tearDown() {
	db := dbConn()
	dbDO(db, `DROP TABLE IF EXISTS events`)
	dbDO(db, `DROP TABLE IF EXISTS views`)
}

func dbDO(db *sql.DB, q string) {
	_, err := db.Exec(q)
	if err != nil {
		panic(err)
	}
}

func setup() *repo.Repo {
	tearDown()
	setupDB()
	return repo.New()
}

func TestUpdate(t *testing.T) {
	dataRepo := setup()

	widget := repo.Widget{Value: "a", ID: "2d3ac602-8ed4-422d-8016-78883a389ed4"}
	err := dataRepo.Create(widget)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	widget = dataRepo.Find(widget.ID)
	widget.Value = "b"
	dataRepo.Update(widget)

	widget = dataRepo.Find(widget.ID)
	if widget.Value != "ab" {
		t.Fatalf("value is %v, should be %v", widget.Value, "ab")
	}
}

func TestDoubleCreate(t *testing.T) {
	dataRepo := setup()

	widget := repo.Widget{Value: "a", ID: "2d3ac602-8ed4-422d-8016-78883a389ed4"}
	err := dataRepo.Create(widget)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	err = dataRepo.Create(widget)
	if err == nil {
		t.Fatalf("expected err but got success")
	}
}

func TestStaleUpdate(t *testing.T) {
	dataRepo := setup()

	widget := repo.Widget{Value: "a", ID: "2d3ac602-8ed4-422d-8016-78883a389ed4"}
	err := dataRepo.Create(widget)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	widget = dataRepo.Find(widget.ID)
	widget.Value = "c"
	dataRepo.Update(widget)

	widget.Value = "b"
	err = dataRepo.Update(widget)
	if err == nil {
		t.Fatalf("expected err but got success")
	}

	widget = dataRepo.Find(widget.ID)
	if widget.Value != "ac" {
		t.Fatalf("value is %v, should be %v", widget.Value, "ac")
	}
}

func TestParallelWriters(t *testing.T) {
	dataRepo := setup()

	widget := repo.Widget{Value: "a", ID: "2d3ac602-8ed4-422d-8016-78883a389ed4"}
	err := dataRepo.Create(widget)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	modifyWidgetTimes(dataRepo, widget.ID, 10)

	widget = dataRepo.Find(widget.ID)
	eventValues := dataRepo.EventValues(widget.ID)

	if widget.Value != eventValues {
		t.Fatalf("value is %v, should be %v", widget.Value, eventValues)
	}
	fmt.Println("widget version: ", widget.Version)
	fmt.Println("widget value: ", widget.Value)
	fmt.Println("event values: ", eventValues)
}

func modifyWidgetTimes(dataRepo *repo.Repo, widgetID string, times int) {
	var wg sync.WaitGroup
	for i := 1; i <= times; i++ {
		wg.Add(1)
		go func(r *repo.Repo, widgetID string) {
			defer wg.Done()
			modifyWidget(dataRepo, widgetID)
		}(dataRepo, widgetID)
	}
	wg.Wait()
}

func modifyWidget(dataRepo *repo.Repo, widgetID string) {
	for i := 1; i <= 10; i++ {
		time.Sleep(10 * time.Millisecond)
		widget := dataRepo.Find(widgetID)
		widget.Value = randomString(1)
		dataRepo.Update(widget)
	}
}

func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return string(bytes)
}
