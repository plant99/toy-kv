package main

import (
	"errors"
	"fmt"
	"os"
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

func shuffleKeysToNewNode(node *WorkerNode) {
	/*
		This function balances some keys in the cluster to the new node using the following steps
		 - Count max(KeyCount) in nodes
		 - All nodes would have elements between max(keycount) - max(keycount) - 1
		 - Every node other than new node would lose [max(keycount) / n-1] elements
		 - New node would gain roughly max(keycount)-c keys
	*/

	maxKeyCount := getMaxKeyCount()
	keyCountToLosePerNode := maxKeyCount / len(nodes)

	// WARN: Optimize how to pick keys to reshuffle
	// Following code linearly iterates and moves keys until max is reached for new node
	relocationCounts := map[string]int{}
	fmt.Println(keyDirectory, node)
	for key, nodeIds := range keyDirectory {
		fmt.Println(key, nodeIds)
		// WARN: error is ignored
		nodeOld, _ := getWorkerWithId(nodeIds[0])
		nodeRelocationCount := relocationCounts[nodeOld.Id]
		if nodeRelocationCount >= keyCountToLosePerNode {
			// continue if already a significant number of keys are moved elsewhere
			continue
		}
		// get kv
		kv, err := requestToWorker(nodeOld.Address, string(key), "", REQUEST_GET)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SHUFFLE: Failed to put key %s at worker %s", key, node.Id)
			continue
		}
		// add key to new node
		_, err = requestToWorker(node.Address, string(key), kv.Value, REQUEST_PUT)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SHUFFLE: Failed to put key %s at worker %s", key, node.Id)
			continue
		}
		// delete key from old node
		_, err = requestToWorker(nodeOld.Address, string(key), "", REQUEST_DELETE)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SHUFFLE: Failed to delete key %s at worker %s", key, node.Id)
		}
		// adjust KeyCount
		node.KeyCount += 1
		nodeOld.KeyCount -= 1

		relocationCounts[nodeOld.Id] = nodeRelocationCount + 1
		keyDirectory[key][0] = node.Id
	}
}

func shuffleKeysFromOldNode(nodeIdOld string) {
	/*
		This function moves keys from the nodes being deleted to other nodes
		- Iterate through all keys (Could be improved by indexing which keys are linked to which node)
		- set nodeIndex = 0
		- If keyDirectory[key] has nodeId
			- get its value from  other nodes
			- save it to nodes[nodesIndex]
			- Add name to KD
			- nodesIndex += 1
			- delete from keyDirectory
	*/

	// Since node with nodeId is already deleted, only nodeId is to be worked with.
	nodeIndex := 0
	for key, nodeIds := range keyDirectory {
		// ignore the key if nodeIdOld not in nodeIds
		if !stringInSlice(nodeIdOld, nodeIds) {
			continue
		}

		// Get value
		// TODO: refactor following part with getHandler in orchestrator.go
		var kv WorkerKVResponse
		for _, nodeId := range nodeIds {
			node, err := getWorkerWithId(nodeId)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get worker with id %s", nodeId)
				continue
			}
			// get KV from node
			kv, err = requestToWorker(node.Address, string(key), "", REQUEST_GET)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get key %s at worker %s", key, node.Id)
				continue
			}
			// break when any one of the storage node responds
			break
		}
		if kv.Value == "" {
			// Corrupt data
			continue
		}

		// add key to new node
		nodeNew := &nodes[nodeIndex]
		nodeIndex++

		_, err := requestToWorker(nodeNew.Address, string(key), kv.Value, REQUEST_PUT)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SHUFFLE: Failed to put key %s at worker %s", key, nodeNew.Id)
			continue
		}
		keyDirectory[key] = append(keyDirectory[key], nodeNew.Id)
		// delete key from old node - a request isn't required.
		deletedNodeIndex := indexOfString(nodeIdOld, keyDirectory[key])
		keyDirectory[key] = removeFromSliceOfStrings(keyDirectory[key], deletedNodeIndex)
		// adjust KeyCount
		nodeNew.KeyCount += 1
	}

}

func getMaxKeyCount() int {
	maxKeyCount := -1
	for _, node := range nodes {
		if node.KeyCount > maxKeyCount {
			maxKeyCount = node.KeyCount
		}
	}
	return maxKeyCount
}

func getWorkerWithId(id string) (*WorkerNode, error) {
	for _, node := range nodes {
		if node.Id == id {
			return &node, nil
		}
	}
	return &WorkerNode{}, errors.New("no worker with given id")
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
