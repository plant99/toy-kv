package main

import "github.com/google/uuid"

// constants
const CMD_TYPE_ORCHESTRATOR = "orch"
const CMD_TYPE_WORKER = "worker"
const CMD_TYPE_CLIENT = "client"

const CMD_ACTION_START = "start"
const CMD_ACTION_STOP = "stop"
const REPLICATION_COUNT = 2

const REQUEST_GET = "GET"
const REQUEST_PUT = "PUT"
const REQUEST_DELETE = "DELETE"

// utility functions
func generateUUID() string {
	return uuid.New().String()
}
