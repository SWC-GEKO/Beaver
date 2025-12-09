package main

import (
	"fmt"
	"log"
	"net"
)

type Merger struct {
	MergeFunction     func(in []chan byte, out chan byte) error
	InputChannels     []chan byte
	OutputChannel     chan byte
	FixedInputSize    int
	InputListenerPort string
	DownstreamIP      string
}

func NewMerger(mergeFunction func(inputChan []chan byte, outChan chan byte) error, fixedInputSize int, inputPort string, downstreamIP string) *Merger {

	inputChans := make([]chan byte, fixedInputSize)
	outputChan := make(chan byte)

	return &Merger{
		MergeFunction:     mergeFunction,
		InputChannels:     inputChans,
		OutputChannel:     outputChan,
		InputListenerPort: inputPort,
		DownstreamIP:      downstreamIP,
		FixedInputSize:    fixedInputSize,
	}
}

// Run starts the Merger's lifecycle. It connects to the downstream operator instance, starts to tcp listeners for the
// upstream processing functions, it currently only accepts a fixed size of incoming connections (which can be merged).
func (m *Merger) Run() error {
	log.Printf("downstream-service-ip: %s", m.DownstreamIP)
	outConn, err := net.Dial("tcp", m.DownstreamIP)
	if err != nil {
		return fmt.Errorf("connecting to downstream service failed with err: %v", err)
	}
	defer outConn.Close()

	inListener, err := net.Listen("tcp", m.InputListenerPort)
	if err != nil {
		return err
	}
	defer inListener.Close()

	inputConnections := make([]net.Conn, m.FixedInputSize)
	for i := 0; i < m.FixedInputSize; i++ {
		// Where do we know which processor-function is 1/2/3/4 ...? -> Maybe Prefix/or Container Names/...
		conn, err := inListener.Accept()
		if err != nil {
			return err
		}

		m.InputChannels[i] = make(chan byte)

		inputConnections[i] = conn
	}

	errChan := make(chan error, 1)

	go func() {
		if err := m.HandleConnections(inputConnections, outConn); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("HandleConnections failed: %w", err)
	}
}

// HandleConnections starts the three core pipelines of the Merger:
// - It spawns for every processing function's incoming connection a thread that reads bytes and pushes them into one of the InputChannels.
// - It uses another thread to let the actual MergeFunction run async
// - It spawns one thread for the writeToOutputConn which forwards bytes from the OutputChannel to the downstream TCP Connection.
func (m *Merger) HandleConnections(inputConnections []net.Conn, outputConn net.Conn) error {
	defer outputConn.Close()

	errChan := make(chan error)
	for i, conn := range inputConnections {
		idx, c := i, conn
		go func() {
			if err := passIncomingBytes(m.InputChannels[idx], c); err != nil {
				errChan <- err
			}
		}()
	}

	go func() {
		if err := m.MergeFunction(m.InputChannels, m.OutputChannel); err != nil {
			errChan <- err
		}
	}()

	go func() {
		if err := writeToOutputConn(m.OutputChannel, outputConn); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		log.Printf("error occured in: %v", err)
		return err
	}
}

// passIncomingBytes reads from a single connection and writes the bytes to the input channel
func passIncomingBytes(ch chan byte, conn net.Conn) error {
	defer conn.Close()

	for {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			return fmt.Errorf("passing incoming bytes of conn to: %v failed with err: %v", conn.RemoteAddr(), err)
		}
		log.Printf("writing %v to chan: %v", buf, ch)
		ch <- buf[0]
	}
}

// writeToOutputConn pulls bytes from the output channel of the merge function and forwards them,
// to the connection to the downstream service.
func writeToOutputConn(ch chan byte, conn net.Conn) error {
	defer conn.Close()
	for {
		b := <-ch
		log.Printf("writer: read a byte from ch")

		_, err := conn.Write([]byte{b})
		if err != nil {
			return err
		}
	}
}
