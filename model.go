package main

import (
	"errors"
	"sort"
)

// orchestrator

type WorkerNode struct {
	Address string
	Id      string
	/*
		this can be collected from KeyDirectory but
		linear search isn't feasible for every operation

		TODO: ValueSizes should be tracked instead of KeyCount since values can be lengthy.
	*/
	KeyCount int
}

func getNWorkersWithMostStorage(workerCount int, nodesC []WorkerNode) ([]WorkerNode, error) {
	/*
		Given M nodes, this functions returns nodes with least KeyCounts
	*/
	sort.Slice(nodesC, func(i, j int) bool {
		return nodesC[i].KeyCount < nodesC[j].KeyCount
	})
	// check if there are enough workers, raise error if not
	if len(nodesC) < workerCount {
		return nil, errors.New("not enough workers")
	}
	return nodesC[0:workerCount], nil
}

func getWorkerWithId(id string) (WorkerNode, error) {
	for _, node := range nodes {
		if node.Id == id {
			return node, nil
		}
	}
	return WorkerNode{}, errors.New("no worker with given id")
}

type WorkerRegistrationResponse struct {
	WorkerId string `json:"worker_id"`
}

type WorkerKVResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Key string
type NodeIds []string
type KeyDirectory map[Key]NodeIds

// worker

type KVStore map[string]string
