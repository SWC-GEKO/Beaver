package merger

import (
	"context"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

func startDownstreamServer(t *testing.T, address string, wg *sync.WaitGroup, expectedBytes int, ch chan byte) {
	defer wg.Done()

	ln, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("unexpected behavior creating a listener for downstream server: %v", err)
	}

	received := 0
	for {
		conn, err := ln.Accept()
		if err != nil {
			t.Errorf("accepting connection in downstream server failed with err: %v", err)
		}

		buf := make([]byte, 1)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				return
			}

			ch <- buf[0]
			received++
			log.Printf("received %d bytes, expected bytes %d", received, expectedBytes)

			if received == expectedBytes {
				log.Printf("downstream received all %d bytes, exiting", expectedBytes)
				return
			}

		}
	}
}

func startUpstreamServer(t *testing.T, address string, rune byte, wg *sync.WaitGroup) {
	defer wg.Done()

	time.Sleep(5 * time.Second)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("unexpected error from dial: %v", err)
	}

	_, err = conn.Write([]byte{rune})
	if err != nil {
		t.Errorf("unexpected error writing to merger: %v", err)
	}
}

func ConcatReverseFunction(ctx context.Context, in []chan byte, out chan byte) error {
	for {
		var outArr []byte

		for _, ch := range in {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case b, ok := <-ch:
				if ok {
					outArr = append(outArr, b)
				}
			default:
			}
		}

		for i := len(outArr) - 1; i >= 0; i-- {
			out <- outArr[i]
		}
	}
}

func TestMerger_Run(t *testing.T) {

	downstreamIP := ":9001"
	mergerIP := ":9000"

	merger := NewMerger(ConcatReverseFunction, 3, mergerIP, downstreamIP)

	var wg sync.WaitGroup

	wg.Add(1)

	expected := 3
	ch := make(chan byte, 3)

	go startDownstreamServer(t, downstreamIP, &wg, expected, ch)

	time.Sleep(1 * time.Second)

	go func() {
		if err := merger.Run(context.WithTimeout(context.Background(), 200*time.Second)); err != nil {
			t.Errorf("merger run returned error: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	upstreamBytes := []byte{'a', 'b', 'c'}
	for _, b := range upstreamBytes {
		wg.Add(1)
		go func(val byte) {
			startUpstreamServer(t, mergerIP, val, &wg)
		}(b)
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)

	log.Printf("only waiting")
	wg.Wait()

	close(ch)

	var got []byte
	for b := range ch {
		got = append(got, b)
	}

	if len(got) != 3 {
		t.Fatalf("downstream did not receive all bytes, got %v", got)
	}
}
