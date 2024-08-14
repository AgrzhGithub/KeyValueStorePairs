package logger

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type PostgresTransactionLog struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

type PostgresDBParams struct {
	host     string
	dbName   string
	user     string
	password string
}

func (l *PostgresTransactionLog) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *PostgresTransactionLog) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}
func (l *PostgresTransactionLog) Err() error {
	return <-l.errors
}

func NewPostgresTransactionLogger(config PostgresDBParams) (*PostgresTransactionLog, error) {

	connStr := fmt.Sprintf("host = %s dbname=%s user=%s password=%s sslmode=disable",
		config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB: %v", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed connection to Postgres: %v", err)
	}

	logger := &PostgresTransactionLog{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %v", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %v", err)
		}
	}
	return logger, nil
}
