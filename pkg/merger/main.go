package main

import (
	"fmt"
	"log"
	"net/http"
)

var inputPort = ":8000"
var controlPort = "9000"

func main() {
	log.SetPrefix("merger: ")
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	// Example Values
	functionCount := 5
	downstreamIP := "host.docker.internal:10000"

	merger := NewMerger(fn, functionCount, inputPort, downstreamIP)

	errChan := make(chan error, 1)
	go func() {
		err := merger.Run()
		if err != nil {
			errChan <- fmt.Errorf("running merger failed with err: %v", err)
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
	case err := <-errChan:
		log.Printf("error will running merger: %v", err)
		return
	}
}
