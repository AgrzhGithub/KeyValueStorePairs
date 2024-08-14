package service

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
	"value/logger"
	"value/types"
)

var store = types.Store
var transact *logger.TransactionLogger

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
	transact.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
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
	transact.WriteDelete(key)
	w.WriteHeader(http.StatusOK)

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

func NewFileTransactionLogger(filename string) (*logger.TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open logger log file: %w", err)
	}
	return &logger.TransactionLogger{File: file}, nil
}

func InitilizeTransactionLog() error {
	var err error

	transact, err = NewFileTransactionLogger("transaction.log")
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
