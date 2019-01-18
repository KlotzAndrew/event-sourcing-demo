package repo

import (
	"database/sql"
	"errors"

	_ "github.com/lib/pq"
)

// Widget the thing we save
type Widget struct {
	ID      string
	Version int
	Value   string
}

// Repo persists widgets
type Repo struct {
	db *sql.DB
}

// New factory function for repo
func New() *Repo {
	db, err := sql.Open("postgres", "user=postgres sslmode=disable")
	must(err)
	return &Repo{db: db}
}

// Create persists widget
func (r *Repo) Create(w Widget) error {
	tx, err := r.db.Begin()
	must(err)

	if err = createView(tx, w.ID, w.Value); err != nil {
		return err
	}
	if err = saveEvent(tx, w.ID, 0, w.Value); err != nil {
		return err
	}
	must(tx.Commit())

	return nil
}

// Update persists widget
func (r *Repo) Update(w Widget) error {
	tx, err := r.db.Begin()
	must(err)

	if err = updateView(tx, w.Version, w.ID, w.Value); err != nil {
		return err
	}
	if err = saveEvent(tx, w.ID, w.Version, w.Value); err != nil {
		return err
	}
	must(tx.Commit())

	return nil
}

// Find widget by id
func (r *Repo) Find(id string) Widget {
	var version int
	var value string
	err := r.db.QueryRow("SELECT version, value FROM views WHERE widget_id = $1", id).Scan(&version, &value)
	must(err)

	return Widget{ID: id, Value: value, Version: version}
}

// EventValues concatinated values from events
func (r *Repo) EventValues(id string) string {
	var values string
	err := r.db.QueryRow("SELECT string_agg(value, '') FROM views WHERE widget_id = $1", id).Scan(&values)
	must(err)

	return values
}

func createView(tx *sql.Tx, widgetID, value string) error {
	res, err := tx.Exec("INSERT INTO views (widget_id, version, value) VALUES ($1, $2, $3) RETURNING widget_id", widgetID, 1, value)
	return checkViewUpdated(tx, res, err)
}

func updateView(tx *sql.Tx, version int, widgetID, value string) error {
	res, err := tx.Exec("UPDATE views SET version = $1, value = CONCAT(value, $2::TEXT) WHERE widget_id = $3 AND version = $4", version+1, value, widgetID, version)
	return checkViewUpdated(tx, res, err)
}

func checkViewUpdated(tx *sql.Tx, res sql.Result, err error) error {
	if err != nil {
		tx.Rollback()
		panic(err)
	}
	count, err := res.RowsAffected()
	must(err)
	if count == 0 {
		tx.Rollback()
		return errors.New("row not updated")
	}
	return nil
}

func saveEvent(tx *sql.Tx, widgetID string, version int, value string) error {
	rows, err := tx.Query("INSERT INTO events (widget_id, version, value) VALUES ($1, $2, $3)", widgetID, version+1, value)
	if err != nil {
		tx.Rollback()
		return err
	}
	rows.Close()

	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
