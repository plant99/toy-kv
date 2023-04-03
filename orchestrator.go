package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

var keyDirectory KeyDirectory
var nodes []WorkerNode

func init() {
	keyDirectory = make(KeyDirectory, 0)
	nodes = make([]WorkerNode, 0)
}

func requestToWorker(workerUrl string, key string, value string, requestType string) (WorkerKVResponse, error) {
	var url, errorStringCustom string

	// Construct URL and errorString
	if requestType == REQUEST_GET {
		url = fmt.Sprintf("%s/get?key=%s", workerUrl, key)
		errorStringCustom = "failed to get key from worker"
	} else if requestType == REQUEST_PUT {
		url = fmt.Sprintf("%s/put?key=%s&value=%s", workerUrl, key, value)
		errorStringCustom = "failed to put key to worker"
	} else if requestType == REQUEST_DELETE {
		url = fmt.Sprintf("%s/delete?key=%s", workerUrl, key)
		errorStringCustom = "failed to delete key from worker"
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return WorkerKVResponse{}, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return WorkerKVResponse{}, err
	}
	if res.StatusCode != http.StatusOK {
		return WorkerKVResponse{}, errors.New(errorStringCustom)
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return WorkerKVResponse{}, err
	}
	// construct kvResponse
	var kvResponse WorkerKVResponse
	err = json.Unmarshal(resBody, &kvResponse)
	if err != nil {
		return WorkerKVResponse{}, err
	}
	return kvResponse, nil
}

func runOrchestrator(host string, port int16, useCheckpoint bool) {
	/*
		Starts orchestrator service.
	*/
	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprint(writer, "Hello from orchestrator")
	})
	http.HandleFunc("/register_worker", handleRegister)
	http.HandleFunc("/deregister_worker", handleDeregister)
	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/delete", handleDelete)

	server := http.Server{
		Addr: fmt.Sprintf("%s:%d", host, port),
	}
	fmt.Println("Starting server")

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

func handleRegister(writer http.ResponseWriter, request *http.Request) {
	workerUrl := request.URL.Query().Get("worker_url")
	if workerUrl == "" {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": "Invalid query parameters."})
		return
	}
	// create workerNode
	workerNode := WorkerNode{
		Address: workerUrl,
		// TODO: Id should be integer to save space.
		Id:       generateUUID(),
		KeyCount: 0,
	}
	nodes = append(nodes, workerNode)

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(WorkerRegistrationResponse{WorkerId: workerNode.Id})

	workerNode = nodes[len(nodes)-1]

	// Ignore errors from shuffling, since load gets readjusted as new keys come in
	shuffleKeysToNewNode(&workerNode)
}

func handleDeregister(writer http.ResponseWriter, request *http.Request) {

	workerId := request.URL.Query().Get("worker_id")
	if workerId == "" {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": "Invalid query parameters."})
		return
	}
	// delete node from nodes with id=workerId

	shuffleKeysFromOldNode(workerId)
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
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
	/*
		Choose REPLICATION_COUNT number of nodes with highest 'storage_capacity', and forward PUT.
		Acquire lock for the key so that other PUT request don't override

		Wait for ACK before releasing the lock.
	*/
	nodesToPut, err := getNWorkersWithMostStorage(REPLICATION_COUNT, nodes)
	if err != nil {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
		return
	}

	keyDirectory[Key(key)] = make(NodeIds, 0)
	for _, node := range nodesToPut {
		// put KV in nodes
		_, err := requestToWorker(node.Address, key, value, REQUEST_PUT)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to save key %s at worker %s", key, node.Id)
			// TODO: have a synchronous retry mechanism because replication is affected if PUT fails
			continue
		}
		keyDirectory[Key(key)] = append(keyDirectory[Key(key)], node.Id)
		// save in keyDirectory
		// increase KeyCount
		node.KeyCount += 1
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(WorkerKVResponse{Key: key, Value: value})
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	key := request.URL.Query().Get("key")
	if key == "" {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": "Invalid query parameters."})
		return
	}
	/*
		Check keyDirectory and get key from node.

		Doesn't need lock.

		Note: not a lot of benefits by making this concurrent and waiting on goroutines,
		unless REPLICATION_COUNT is huge.
	*/
	nodeIdsForKey := keyDirectory[Key(key)]
	if len(nodeIdsForKey) == 0 {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusNotFound)
		json.NewEncoder(writer).Encode(map[string]string{"error": "key not found"})
		return
	}
	for _, nodeId := range nodeIdsForKey {
		node, err := getWorkerWithId(nodeId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get worker with id %s", nodeId)
			continue
		}
		// get KV from node
		kv, err := requestToWorker(node.Address, key, "", REQUEST_GET)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get key %s at worker %s", key, node.Id)
			continue
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		json.NewEncoder(writer).Encode(kv)
		// break when any one of the storage node responds
		break
	}
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	key := request.URL.Query().Get("key")
	if key == "" {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": "Invalid query parameters."})
		return
	}
	/*
		Check keyDirectory and delete keys from _all_ nodes.

		Needs lock.
	*/

	nodeIdsForKey := keyDirectory[Key(key)]
	if len(nodeIdsForKey) == 0 {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusNotFound)
		json.NewEncoder(writer).Encode(map[string]string{"error": "key not found"})
	}
	var kv WorkerKVResponse
	for _, nodeId := range nodeIdsForKey {
		node, err := getWorkerWithId(nodeId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get worker with id %s", nodeId)
			continue
		}
		// get KV from node
		kv, err = requestToWorker(node.Address, key, "", REQUEST_DELETE)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to delete key %s at worker %s", key, node.Id)
			continue
		}
		// Decrease Node.KeyCount
		node.KeyCount -= 1
	}
	// delete keyDirectory[key]
	delete(keyDirectory, Key(key))
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(kv)
}
