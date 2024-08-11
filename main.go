package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"value/client"
)

var keyValuePutHandler = client.KeyValuePutHandler
var keyValueGetHandler = client.KeyValueGetHandler
var keyValueDeleteHandler = client.KeyValueDeleteHandler

func main() {

	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DEL")

	log.Fatal(http.ListenAndServe(":8080", r))

}
