package main

import (
	"fmt"
	"log"
	"net"
)

type Handler struct {
	InputPort string
	OutputIP  string
}

func NewHandler(inputPort string, outputIP string) *Handler {
	return &Handler{
		InputPort: inputPort,
		OutputIP:  outputIP,
	}
}

func (h *Handler) Run() error {

	inputListener, err := net.Listen("tcp", h.InputPort)
	if err != nil {
		return fmt.Errorf("listening from port: %s failed with err: %v", h.InputPort, err)
	}

	log.Printf("listening on: %v", inputListener.Addr())

	outputConn, err := net.Dial("tcp", h.OutputIP+":8000")
	if err != nil {
		return fmt.Errorf("dialing merger failed with err: %v", err)
	}
	log.Printf("successfully connected to merger: %v", outputConn.RemoteAddr())

	inputConn, err := inputListener.Accept()
	if err != nil {
		return fmt.Errorf("accepting incoming conn failed with err: %v", err)
	}

	errChan := make(chan error)
	go func() {
		if err := h.HandleConnection(inputConn, outputConn); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	}

}

func (h *Handler) HandleConnection(inputConn net.Conn, outputConn net.Conn) error {

	for {
		buf := make([]byte, 1)
		_, err := inputConn.Read(buf)
		if err != nil {
			return err
		}

		res := fn(buf)

		_, err = outputConn.Write(res)
		if err != nil {
			return err
		}
	}
}
