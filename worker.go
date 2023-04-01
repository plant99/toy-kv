package main

import (
	"fmt"
	"net/http"
)

func runWorker(host string, port int16, serverUrl string) {
	/*
		Starts worker service
	*/
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprint(writer, "Hello from worker")
	})
	server := http.Server{
		Addr: fmt.Sprintf("%s:%d", host, port),
	}
	fmt.Println("Starting server")

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
