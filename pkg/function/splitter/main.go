package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
)

var controlPort = "9002"
var inputStreamPort = ":8000"

func main() {
	log.Println("Starting to discover functions...")
	functionIPs, err := DiscoverFunctions(5)
	if err != nil {
		log.Fatalf("discovering functions failed with err: %v", err)
	}

	splitter := NewSplitter(fn, len(functionIPs), inputStreamPort, functionIPs)

	errChan := make(chan error, 1)
	go func() {
		err = splitter.Run()
		if err != nil {
			errChan <- fmt.Errorf("running splitter failed with err: %v", err)
		}
	}()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path == "/health" {
				writer.WriteHeader(http.StatusOK)
				log.Println("reporting health: OK")
				return
			}
			writer.WriteHeader(http.StatusNotFound)
			return
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})

	if err := http.ListenAndServe(":"+controlPort, nil); err != nil {
		log.Fatalf("control http-server failed with err: %v", err)
	}

	select {
	case err = <-errChan:
		log.Println(err)
		return
	}
}

func DiscoverFunctions(initialFunctionCount int) ([]string, error) {
	found := 0
	for found < initialFunctionCount {
		fnIPs, err := net.LookupHost("processor")
		if err != nil {
			return nil, err
		}

		found = len(fnIPs)

		if found > initialFunctionCount {
			return nil, fmt.Errorf("found more functions than expected, aborting")
		}

		if found == initialFunctionCount {
			log.Printf("found: %v", fnIPs)
			return fnIPs, nil
		}

	}

	return []string{}, fmt.Errorf("weird behavior, aborting")
}
