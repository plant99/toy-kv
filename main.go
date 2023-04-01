package main

import (
	"flag"
	"fmt"
)

func main() {
	/*
		CLI entrypoint to interact with toy-kv.
	*/
	cmdType := flag.String("type", CMD_TYPE_CLIENT, fmt.Sprintf("Valid parameters are %s, %s, %s.", CMD_TYPE_CLIENT, CMD_TYPE_ORCHESTRATOR, CMD_TYPE_WORKER))
	cmdAction := flag.String("action", CMD_ACTION_START, fmt.Sprintf("Valid parameters are %s, %s.", CMD_ACTION_START, CMD_ACTION_STOP))
	flag.Parse()
	if *cmdType == CMD_TYPE_ORCHESTRATOR {
		if *cmdAction == CMD_ACTION_START {
			runOrchestrator("0.0.0.0", 8000)
		}
	} else if *cmdType == CMD_TYPE_WORKER {
		if *cmdAction == CMD_ACTION_START {
			runWorker("0.0.0.0", 4000, "http://127.0.0.1:8000")
		}
	}
}
