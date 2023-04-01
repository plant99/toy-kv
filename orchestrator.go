package main

import (
	"fmt"
	"net/http"
)

func runOrchestrator(host string, port int16) {
	/*
		Starts orchestrator service.
	*/
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprint(writer, "Hello from orchestrator")
	})
	server := http.Server{
		Addr: fmt.Sprintf("%s:%d", host, port),
	}
	fmt.Println("Starting server")

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
