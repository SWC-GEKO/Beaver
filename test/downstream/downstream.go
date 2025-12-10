package main

import (
	"io"
	"log"
	"net"
	"os"
)

var filePath = "./test/test-file.txt"

func main() {
	log.SetPrefix("downstream: ")
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	ln, err := net.Listen("tcp", ":10000")
	if err != nil {
		log.Fatalf("listening to :10000 failed: %v", err)
	}
	log.Printf("started listener on: %v", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("accepting connection failed: %v", err)
		}
		defer conn.Close()

		log.Printf("received connection: %v", conn.RemoteAddr())

		f, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("error creating file: %s", filePath)
		}
		defer f.Close()

		go func() {
			err = Run(conn, f)
			if err != nil {
				log.Printf("running downstream service failed: %v", err)
			}
		}()

	}
}

func Run(conn net.Conn, f *os.File) error {
	buf := make([]byte, 1)

	for {
		i, err := conn.Read(buf)

		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("read %d bytes", i)

		_, err = f.WriteString(string(buf))
		if err != nil {
			return err
		}
	}
}
