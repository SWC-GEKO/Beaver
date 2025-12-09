package main

import (
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

var functionIP = "localhost:8000"
var aliceFilePath = "./test/wonderland.txt"

func main() {

	txt, err := os.ReadFile(aliceFilePath)
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := net.Dial("tcp", functionIP)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	count := 0
	lastLog := time.Now()
	for _, r := range txt {
		_, err := conn.Write([]byte{r})
		if err != nil {
			log.Fatalln(err)
		}

		count++

		if rand.Float64() <= 0.05 {
			log.Printf("wrote %d in: %v", count, time.Since(lastLog))
			count = 0
			lastLog = time.Now()
			time.Sleep(100 * time.Millisecond)
		}
	}
}
