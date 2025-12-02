package merger

import (
	"context"
	"fmt"
	"log"
	"net"
)

type Merger struct {
	MergeFunction     func(ctx context.Context, in []chan byte, out chan byte) error
	InputChannels     []chan byte
	OutputChannel     chan byte
	FixedInputSize    int
	InputListenerPort string
	DownstreamIP      string
}

func NewMerger(mergeFunction func(ctx context.Context, inputChan []chan byte, outChan chan byte) error, fixedInputSize int, inputPort string, downstreamIP string) *Merger {

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

func (m *Merger) Run(ctx context.Context, cancelFunc context.CancelFunc) error {
	defer cancelFunc()

	outConn, err := net.Dial("tcp", m.DownstreamIP)
	if err != nil {
		return err
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
		// Why does this not block here?
		conn, err := inListener.Accept()
		if err != nil {
			return err
		}

		log.Printf("successfully connected to input connection with addr: %v", conn.RemoteAddr())

		m.InputChannels[i] = make(chan byte)

		inputConnections[i] = conn
	}

	errChan := make(chan error, 1)

	go func() {
		if err := m.HandleConnections(ctx, inputConnections, outConn); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("HandleConnections failed: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Merger) HandleConnections(ctx context.Context, inputConnections []net.Conn, outputConn net.Conn) error {
	defer outputConn.Close()

	errChan := make(chan error)
	for i, conn := range inputConnections {
		log.Printf("%d connection is: %v", i, conn.RemoteAddr())
		idx, c := i, conn
		go func() {
			if err := passIncomingBytes(ctx, c, m.InputChannels[idx]); err != nil {
				errChan <- err
			}
		}()
	}

	go func() {
		if err := m.MergeFunction(ctx, m.InputChannels, m.OutputChannel); err != nil {
			errChan <- err
		}
	}()

	go func() {
		if err := writeToOutputConn(ctx, m.OutputChannel, outputConn); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		log.Printf("error occured in: %v", err)
		return err
	case <-ctx.Done():
	}

	return nil
}

func passIncomingBytes(ctx context.Context, conn net.Conn, ch chan byte) error {
	defer conn.Close()

	log.Printf("starting to pass incoming bytes")
	buf := make([]byte, 1)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, err := conn.Read(buf)
		if err != nil {
			return fmt.Errorf("passing incoming bytes of conn to: %v failed with err: %v", conn.RemoteAddr(), err)
		}

		log.Printf("red byte: %s passing it to chan: %v", string(buf[0]), ch)
		ch <- buf[0]
	}
}

func writeToOutputConn(ctx context.Context, ch chan byte, conn net.Conn) error {
	defer conn.Close()
	for {
		select {
		case b, ok := <-ch:
			if !ok {
				return nil
			}
			_, err := conn.Write([]byte{b})
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
