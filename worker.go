package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
)

var kvStore KVStore
var workerId string

func init() {
	kvStore = make(map[string]string)
}

func registerWorkerNode(serverUrl string, workerUrl string) error {
	// call serverUrl on /register
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/register_worker?worker_url=%s", serverUrl, workerUrl), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		fmt.Println(res)
		return errors.New("failed to register worker")
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	var workerInformation WorkerRegistrationResponse
	err = json.Unmarshal(resBody, &workerInformation)
	if err != nil {
		return err
	}
	workerId = workerInformation.WorkerId
	fmt.Println("Registered worker with ID:", workerId)
	return nil
}

func deregisterWorkerNode(serverUrl string) error {
	// call serverUrl on /register
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/deregister_worker?worker_id=%s", serverUrl, workerId), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return errors.New("failed to deregister worker")
	}
	fmt.Println("Deregistered worker with ID:", workerId)
	return nil
}

func runWorker(host string, port int16, serverUrl string) {
	/*
		Listening to interrupts to deregister
	*/
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			// add retries
			deregisterWorkerNode(serverUrl)
			os.Exit(1)
		}
	}()

	/*
		Starts worker service
	*/
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprint(writer, "Hello from worker")
	})
	http.HandleFunc("/put", func(writer http.ResponseWriter, request *http.Request) {
		// TODO: change to PUT request
		// get KV
		key := request.URL.Query().Get("key")
		value := request.URL.Query().Get("value")
		if key == "" || value == "" {
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(writer).Encode(map[string]string{"error": "Invalid query parameters."})
			return
		}

		// add to kvStore
		kvStore[key] = value
		writer.WriteHeader(http.StatusOK)
		writer.Header().Set("Content-Type", "application/json")
		data := WorkerKVResponse{
			Key:   key,
			Value: value,
		}
		json.NewEncoder(writer).Encode(data)
	})
	http.HandleFunc("/put_batch", func(writer http.ResponseWriter, request *http.Request) {
		// TODO: implement batch input which occurs when orchestrator registers this node
	})
	http.HandleFunc("/get", func(writer http.ResponseWriter, request *http.Request) {
		// get KV
		key := request.URL.Query().Get("key")
		if key != "" {
			// add to kvStore
			value, ok := kvStore[key]
			if !ok {
				// return 404
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusNotFound)
				json.NewEncoder(writer).Encode(map[string]string{"error": "key not found"})
				return
			}
			writer.WriteHeader(http.StatusOK)
			data := WorkerKVResponse{
				Key:   key,
				Value: value,
			}
			json.NewEncoder(writer).Encode(data)
		}
	})
	http.HandleFunc("/delete", func(writer http.ResponseWriter, request *http.Request) {
		// get KV
		key := request.URL.Query().Get("key")
		if key != "" {
			value, ok := kvStore[key]
			if !ok {
				// return 404
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusNotFound)
				json.NewEncoder(writer).Encode(map[string]string{"error": "key not found"})
				return
			}
			// delete key
			delete(kvStore, key)
			// set OK status
			writer.WriteHeader(http.StatusOK)
			data := WorkerKVResponse{
				Key:   key,
				Value: value,
			}
			json.NewEncoder(writer).Encode(data)
		}
	})
	server := http.Server{
		Addr: fmt.Sprintf("%s:%d", host, port),
	}

	if err := registerWorkerNode(serverUrl, fmt.Sprintf("http://127.0.0.1:%d", port)); err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while registering: %s", err.Error())
		return
	}
	fmt.Printf("Listening at %s:%d", host, port)

	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while registering: %s", err.Error())
	}
}
