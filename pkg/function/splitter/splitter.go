package main

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net"
	"platform/pkg/function/router"
	"platform/pkg/function/utils"
	"sync"
	"time"
)

type Splitter struct {
	opts *SplitterOpts

	SplitFunction func(context.Context, chan []byte, []chan []byte) error
	outputMu      sync.RWMutex
	readyCh       chan struct{}
	outputs       map[string]*router.Stream
	InputStream   *router.Stream
	router        *router.Router[*SplitInstance]
	bufferPool    sync.Pool
}

func NewSplitter(
	opts *SplitterOpts,
	splitFunction func(context.Context, chan []byte, []chan []byte) error,
) *Splitter {

	s := Splitter{
		opts: opts,
	}

	s.outputs = make(map[string]*router.Stream, s.opts.MaxOutputFunctions)

	s.bufferPool = sync.Pool{
		New: func() interface{} {
			return &utils.PooledBuffer{
				Data: make([]byte, opts.ChunkSize),
				Pool: &s.bufferPool,
			}
		},
	}

	s.readyCh = make(chan struct{})
	s.SplitFunction = splitFunction

	return &s
}

func (s *Splitter) DiscoverFunctions() (map[string]*router.Stream, error) {
	functionIPs, err := net.LookupHost(s.opts.FunctionName)
	if err != nil {
		return nil, err
	}

	functions := make(map[string]*router.Stream, len(functionIPs))
	for _, ip := range functionIPs {
		fn, err := s.connectToFunction(ip)
		if err != nil {
			return nil, err
		}

		functions[ip] = fn

		go s.Writer(fn)
	}

	return functions, nil
}

func (s *Splitter) connectToFunction(ip string) (*router.Stream, error) {
	conn, err := net.Dial("tcp", fmt.Sprint(ip, s.opts.FunctionPort))
	if err != nil {
		return nil, err
	}

	s.opts.TuneTCP(conn)

	stream := router.NewStream(ip, conn, s.opts.ChunkSize)
	return stream, nil
}

func (s *Splitter) Run() error {
	inputListener, err := net.Listen("tcp", s.opts.InputListenerAddr)
	if err != nil {
		return fmt.Errorf("connecting to upstream operator failed with error: %v", err)
	}
	defer inputListener.Close()

	log.Printf("started listening on: %v", inputListener.Addr())

	upstreamConn, err := inputListener.Accept()
	if err != nil {
		return fmt.Errorf("accepting upstream connection failed with error: %v", err)
	}

	s.opts.TuneTCP(upstreamConn)
	s.InputStream = router.NewStream(upstreamConn.RemoteAddr().String(), upstreamConn, s.opts.ChunkSize)

	go s.HandleConsumers()

	go s.HandleProducer()

	go s.HandleSplitFunction()

	return nil
}

func (s *Splitter) HandleConsumers() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			functions, err := s.DiscoverFunctions() // connects to function as well
			if err != nil {
				return
			}

			if maps.Equal(s.outputs, functions) {
				continue
			}

			s.outputMu.Lock()
			s.outputs = functions
			s.outputMu.Unlock()
		}
	}
}

func (s *Splitter) Writer(stream *router.Stream) {
	defer close(stream.Ch)
	for {
		chunk, ok := <-stream.Ch
		if !ok {
			return
		}

		if s.opts.WriterTimeout > 0 {
			stream.Conn.SetWriteDeadline(time.Now().Add(s.opts.WriterTimeout))
		}

		n, err := stream.Conn.Write(chunk)
		if err != nil {
			return
		}

		stream.StreamedBytes += int64(n)
	}
}

func (s *Splitter) HandleSplitFunction() error {
	s.router = router.NewRouter[*SplitInstance]()

	instanceCounter := 0

	for {
		snapshot := s.snapshotConsumers()

		if len(snapshot) == 0 {
			log.Printf("no outputs available")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		instance := s.CreateInstance(instanceCounter, snapshot)

		s.router.Mu.Lock()
		s.router.Instances = append(s.router.Instances, &instance)
		s.router.Mu.Unlock()

		go func(inst *SplitInstance) {
			defer inst.ShutdownInstance()

			if err := s.SplitFunction(inst.ctx, s.InputStream.Ch, inst.outputChans); err != nil {
				log.Printf("error occurred when executing split function [%s]: %v", inst.id, err)
				return
			}

			log.Printf("[%s] split function exited", inst.id)

		}(instance)

		ticker := time.NewTicker(100 * time.Millisecond)

	monitorLoop:
		for {
			select {
			case <-ticker.C:
				s.outputMu.Lock()
				newLen := len(s.outputs)
				s.outputMu.Unlock()

				if newLen != len(snapshot) {
					instance.cancel()

					ticker.Stop()
					break monitorLoop
				}
			}
		}
	}
}

func (s *Splitter) CreateInstance(counter int, snapshot map[string]*router.Stream) *SplitInstance {
	instanceID := fmt.Sprintf("splitter-%d", counter)

	ctx, cancel := context.WithCancel(context.Background())

	outputChans := make([]chan []byte, 0, len(snapshot))
	consumerIDs := make([]string, 0, len(snapshot))

	for id, stream := range snapshot {
		outputChans = append(outputChans, stream.Ch)
		consumerIDs = append(consumerIDs, id)
	}

	instance := SplitInstance{
		id:          instanceID,
		ctx:         ctx,
		cancel:      cancel,
		outputChans: outputChans,
		consumerIDs: consumerIDs,
	}

	return &instance
}

func (s *Splitter) snapshotConsumers() map[string]*router.Stream {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()

	snapshotConsumer := make(map[string]*router.Stream, len(s.outputs))
	for id, stream := range s.outputs {
		snapshotConsumer[id] = stream
	}

	return snapshotConsumer
}

func (s *Splitter) HandleProducer() {
	go s.Reader(s.InputStream)

	<-s.InputStream.Closed
}

func (s *Splitter) Reader(stream *router.Stream) {
	defer close(stream.Closed)

	for {
		pooledBuf := s.getBuffer()

		n, err := stream.Conn.Read(pooledBuf.Data)
		if n > 0 {
			pooledBuf.Length = n
			s.router.RouteData(stream.ID, pooledBuf)
		} else {
			pooledBuf.Release()
		}

		if err != nil {
			s.router.HandleClosedStream(stream.ID)
			return
		}
	}
}

func (s *Splitter) getBuffer() *utils.PooledBuffer {
	return s.bufferPool.Get().(*utils.PooledBuffer)
}
