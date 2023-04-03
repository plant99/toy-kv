package main

import (
	"flag"
	"fmt"
)

func main() {
	/*
		CLI entrypoint to interact with toy-kv.
	*/

	// TODO: Use subcommands with flag - https://gobyexample.com/command-line-subcommands
	cmdType := flag.String("type", "", fmt.Sprintf("Valid parameters are %s, %s, %s.", CMD_TYPE_CLIENT, CMD_TYPE_ORCHESTRATOR, CMD_TYPE_WORKER))
	cmdAction := flag.String("action", "", fmt.Sprintf("Valid parameters are %s, %s.", CMD_ACTION_START, CMD_ACTION_STOP))
	port := flag.Int("port", -1, "Port to be used by the service.")
	serverURL := flag.String("serverURL", "", "Server URL if using WORKER/CLIENT mode.")
	useCheckpoint := flag.Bool("useCheckpoint", false, "If orchestrator should use saved checkpoint.")
	flag.Parse()
	if *cmdType == CMD_TYPE_ORCHESTRATOR {
		// loadFromCheckpoint bool - mention reason for persistence
		if *cmdAction == CMD_ACTION_START {
			runOrchestrator("0.0.0.0", int16(*port), *useCheckpoint)
		}
	} else if *cmdType == CMD_TYPE_WORKER {

		if *cmdAction == CMD_ACTION_START {
			runWorker("0.0.0.0", int16(*port), *serverURL)
		}
	} else if *cmdType == CMD_TYPE_CLIENT {
		// GET, SET, DELETE
		// key, value
		if *cmdAction == CMD_ACTION_START {
			// not implemented
			fmt.Println("Not implemented.")
		}
	} else {
		// print help string
		flag.Usage()
	}
}
