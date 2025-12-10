package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

var controlPort = "9001"

func main() {
	log.SetPrefix("fn: ")
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	mergerIP, err := DiscoverMerger(5)
	if err != nil {
		log.Fatalf("discovering merger failed with err: %v", err)
	}

	handler := NewHandler(":8000", mergerIP)

	errChan := make(chan error, 1)
	go func() {
		err = handler.Run()
		if err != nil {
			errChan <- fmt.Errorf("running handler failed with err: %v", err)
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

func DiscoverMerger(maxRetries int) (string, error) {
	retries := 0
	for {
		peerIPs, err := net.LookupHost("merger")
		if err != nil {
			return "", err
		}

		if len(peerIPs) > 1 {
			return "", fmt.Errorf("more than one merger in the network, rejecting execution")
		}

		if len(peerIPs) == 1 {
			return peerIPs[0], nil
		}
		retries++
		if retries >= maxRetries {
			return "", fmt.Errorf("not able to find merger")
		}
		time.Sleep(100)
	}
}
