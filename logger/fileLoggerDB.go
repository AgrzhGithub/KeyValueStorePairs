package logger

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"sync"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
	wg     *sync.WaitGroup
}

type PostgresDBParams struct {
	Host     string
	Port     string
	DbName   string
	User     string
	Password string
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventDelete, Key: key}
}
func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {

	connStr := fmt.Sprintf("host = %s dbname=%s user=%s password=%s sslmode=disable",
		config.Host, config.DbName, config.User, config.Password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create DB: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to opendb connection: %v", err)
	}

	logger := &PostgresTransactionLogger{db: db, wg: &sync.WaitGroup{}}

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

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transaction"

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s')", table))
	defer rows.Close()

	if err != nil {
		return false, err
	}
	for rows.Next() && result != table {
		rows.Scan(&result)
	}
	return result == table, rows.Err()

}

func (l *PostgresTransactionLogger) createTable() error {
	var err error

	createQuery := `CREATE TABLE transactions(
    sequence BIGSERIAL PRIMARY KEY,
    event_type SMALLINT,
    key TEXT,
    value TEXT
	);`
	_, err = l.db.Exec(createQuery)
	if err != nil {
		return err
	}
	return nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {

		query := `INSERT INTO transactions
    			  (event_type, key, value)
				  VALUES ($1, $2, $3)`

		for e := range events {
			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
			l.wg.Done()
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	query := "SELECT sequence, event_type, key, value FROM transactions"

	go func() {
		defer close(outEvent)
		defer close(outError)

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		e := Event{}
		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}
			outEvent <- e
		}
		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()
	return outEvent, outError
}

func (l *PostgresTransactionLogger) Wait() {
	l.wg.Wait()
}

func (l *PostgresTransactionLogger) Close() error {
	l.Wait()
	if l.events != nil {
		close(l.events)
	}
	return l.db.Close()
}
