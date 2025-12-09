package main

import (
	"fmt"
	"log"
	"net"
)

type Splitter struct {
	SplitFunction     func(in chan byte, out []chan byte) error
	InputChannel      chan byte
	OutputChannels    []chan byte
	FixedFunctions    int
	InputListenerPort string
	FunctionsIPs      []string
}

func NewSplitter(
	splitFunction func(chan byte, []chan byte) error,
	fixedFunction int,
	inputListenerPort string,
	functionIPs []string,
) *Splitter {

	inputChannel := make(chan byte)
	outputChannels := make([]chan byte, fixedFunction)

	for i := 0; i < len(outputChannels); i++ {
		outputChannels[i] = make(chan byte)
	}

	return &Splitter{
		SplitFunction:     splitFunction,
		InputChannel:      inputChannel,
		OutputChannels:    outputChannels,
		FixedFunctions:    fixedFunction,
		InputListenerPort: inputListenerPort,
		FunctionsIPs:      functionIPs,
	}
}

func (s *Splitter) Run() error {

	inputListener, err := net.Listen("tcp", s.InputListenerPort)
	if err != nil {
		return fmt.Errorf("connecting to upstream operator failed with error: %v", err)
	}
	defer inputListener.Close()

	log.Printf("started listening on: %v", inputListener.Addr())

	var fnConnections []net.Conn
	for _, fn := range s.FunctionsIPs {
		fnConn, err := net.Dial("tcp", fn+":8000")
		if err != nil {
			for _, c := range fnConnections {
				c.Close()
			}
			return fmt.Errorf("connecting to function failed with err: %v", err)
		}
		defer fnConn.Close()

		fnConnections = append(fnConnections, fnConn)
	}

	upstreamConn, err := inputListener.Accept()
	if err != nil {
		return fmt.Errorf("accepting upstream connection failed with error: %v", err)
	}

	if err := s.HandleConnections(upstreamConn, fnConnections); err != nil {
		return fmt.Errorf("handling connections failed with error")
	}

	return nil
}

func (s *Splitter) HandleConnections(upstreamConn net.Conn, functionConns []net.Conn) error {

	errChan := make(chan error)
	go func() {
		log.Printf("Input-Channel: %v", s.InputChannel)
		if err := handleIncomingBytes(upstreamConn, s.InputChannel); err != nil {
			errChan <- err
		}
	}()

	go func() {
		log.Printf("starting the split function: InputChan: %v, OutputChans: %v", s.InputChannel, s.OutputChannels)
		if err := s.SplitFunction(s.InputChannel, s.OutputChannels); err != nil {
			errChan <- err
		}
	}()

	// This will cause a problem when our len(outputChannels) > len(fnConns)
	for i, ch := range s.OutputChannels {
		fn := functionConns[i]
		channel := ch
		go func() {
			if err := writeBytesToFunction(channel, fn); err != nil {
				errChan <- err
			}
		}()
	}

	select {
	case err := <-errChan:
		log.Printf("error occured in: %v", err)
		return err
	}
}

func handleIncomingBytes(upstreamConn net.Conn, inputChan chan byte) error {
	for {
		buf := make([]byte, 1)
		_, err := upstreamConn.Read(buf)
		if err != nil {
			return fmt.Errorf("reading from upstream connection failed with error: %v", err)
		}

		inputChan <- buf[0]
	}
}

func writeBytesToFunction(outputChan chan byte, functionConn net.Conn) error {
	for {
		b, ok := <-outputChan
		if !ok {
			log.Printf("output: reading from outputChan failed")
			return nil
		}

		_, err := functionConn.Write([]byte{b})
		if err != nil {
			return fmt.Errorf("writing to function with IP: %s failed with err: %v", functionConn.RemoteAddr(), err)
		}
	}
}
