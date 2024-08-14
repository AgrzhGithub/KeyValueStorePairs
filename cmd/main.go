package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"value/service"
)

var keyValuePutHandler = service.KeyValuePutHandler
var keyValueGetHandler = service.KeyValueGetHandler
var keyValueDeleteHandler = service.KeyValueDeleteHandler
var helloMuxHandler = service.HelloMuxHandler

func main() {

	err := service.InitilizeTransactionLog()
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", helloMuxHandler)
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DEL")

	log.Fatal(http.ListenAndServe(":8080", r))

}
