package service

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"value/logger"
	"value/types"
)

var store = types.Store
var transact logger.TransactionLogger

func HelloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello World!"))
}

func KeyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	transact.WritePut(key, string(value))
	log.Printf("PUT key=%s value=%s", key, string(value))

}

func KeyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w,
			err.Error(),
			http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
	log.Printf("GET key=%s", key)
}

func KeyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	key := vars["key"]

	err := DeleteKey(key)
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	transact.WriteDelete(key)

}

func Put(key, value string) error {
	store.Lock()
	store.M[key] = value
	store.Unlock()
	return nil
}

var ErrorNoSuchKey = errors.New("No such key")

func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.M[key]
	store.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func DeleteKey(key string) error {
	store.Lock()
	delete(store.M, key)
	store.Unlock()
	return nil
}

func InitializeTransactionLog() error {
	var err error

	// Create new DB
	//transact, err = logger.NewPostgresTransactionLogger(logger.PostgresDBParams{
	//	Host:     "127.0.0.1",
	//	Port:     "8080",
	//	DbName:   "kvs",
	//	User:     "test",
	//	Password: "hunter2",
	//})

	// Create new transaction file
	transact, err = logger.NewFileTransactionLogger("transaction.log")

	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	e, ok := logger.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case logger.EventDelete:
				err = DeleteKey(e.Key)
			case logger.EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	transact.Run()
	return err
}
