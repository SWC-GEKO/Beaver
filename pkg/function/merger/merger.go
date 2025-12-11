package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"platform/pkg/function/router"
	"platform/pkg/function/utils"
	"sync"
	"time"
)

type Merger struct {
	opts *MergerOpts

	MergeFunction func(context.Context, []chan []byte, chan []byte) error
	inputsMu      sync.RWMutex
	readyInit     sync.Once
	readyCh       chan struct{}
	inputs        map[string]*router.Stream
	acceptSem     chan struct{}
	OutputStream  *router.Stream
	router        *router.Router[*MergeInstance]
	bufferPool    sync.Pool

	metricsTicker *time.Ticker
	lastLogTime   time.Time
}

func NewMerger(opts *MergerOpts, mergeFunction func(context.Context, []chan []byte, chan []byte) error) *Merger {

	m := Merger{
		opts: opts,
	}

	m.inputs = make(map[string]*router.Stream)
	m.acceptSem = make(chan struct{}, opts.MaxInputFunctions)

	m.bufferPool = sync.Pool{
		New: func() interface{} {
			return &utils.PooledBuffer{
				Data: make([]byte, opts.ChunkSize),
				Pool: &m.bufferPool,
			}
		},
	}

	m.readyCh = make(chan struct{})
	m.MergeFunction = mergeFunction

	return &m
}

// Run handles the Merger's lifecycle. It first connects to the downstream function (can be a chained function, client, ...)
// Then the connection gets "tuned" using the config of the merger, after that the writer starts writing to the downstream connection.
func (m *Merger) Run() error {
	log.Printf("started running merger")
	out, err := net.Dial("tcp", m.opts.DownstreamAddr)
	if err != nil {
		return err
	}

	m.opts.TuneTCP(out)

	m.OutputStream = router.NewStream(out.RemoteAddr().String(), out, m.opts.ChannelBufferSize)

	go m.writer()
	go func() {
		defer out.Close()
		<-m.OutputStream.Closed
	}()

	listener, err := net.Listen("tcp", m.opts.InputListenerAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	errChan := make(chan error)
	go func() {
		err := m.acceptLoop(listener)
		if err != nil {
			errChan <- err
		}
	}()

	<-m.readyCh

	go func() {
		if err := m.handleMergeFunction(); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return fmt.Errorf("HandleConnections failed: %w", err)
	}
}

func (m *Merger) acceptLoop(listener net.Listener) error {
	for {
		select {
		case m.acceptSem <- struct{}{}:
		case <-time.After(m.opts.AcceptTimeout):
			continue
		}

		conn, err := listener.Accept()
		if err != nil {
			<-m.acceptSem
			return err
		}

		go m.handleNewProducer(conn)
	}
}

func (m *Merger) handleMergeFunction() error {
	m.router = router.NewRouter[*MergeInstance]()

	instanceCounter := 0

	for {

		snapshot := m.snapshotProducers()

		// Switch from busy waiting
		if len(snapshot) == 0 {
			log.Printf("no inputs available")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		instance := m.CreateInstance(instanceCounter, snapshot)
		instanceCounter++

		m.router.Mu.Lock()
		m.router.Instances = append(m.router.Instances, &instance)
		m.router.Mu.Unlock()

		go func(inst *MergeInstance) {
			defer inst.ShutdownInstance()

			log.Printf("[%s] invoking user merge function", inst.id)
			if err := m.MergeFunction(inst.ctx, inst.inputChans, m.OutputStream.Ch); err != nil {
				log.Printf("[%s] merge function error: %v", inst.id, err)
			}

			log.Printf("[%s] merge function exited", inst.id)
		}(instance)

		ticker := time.NewTicker(100 * time.Millisecond)
	monitorLoop:
		for {
			select {
			case <-ticker.C:
				m.inputsMu.Lock()
				newLen := len(m.inputs)
				m.inputsMu.Unlock()

				if newLen != len(snapshot) {
					instance.cancel()

					ticker.Stop()
					break monitorLoop
				}
			}
		}
	}
}

func (m *Merger) handleNewProducer(conn net.Conn) {
	defer func() {
		<-m.acceptSem
		conn.Close()
	}()

	log.Printf("started handling new producer")

	id := conn.RemoteAddr().String()

	stream := router.NewStream(id, conn, m.opts.ChannelBufferSize)

	m.inputsMu.Lock()
	m.inputs[id] = stream
	count := len(m.inputs)
	m.inputsMu.Unlock()

	if count >= m.opts.InitialInputFunctions {
		m.readyInit.Do(func() {
			close(m.readyCh)
		})
	}

	go m.reader(stream)

	<-stream.Closed

	m.inputsMu.Lock()
	delete(m.inputs, id)
	m.inputsMu.Unlock()
}

func (m *Merger) reader(s *router.Stream) {
	defer close(s.Closed)

	log.Printf("starting to read from incoming conn: %v", s.ID)

	for {

		pooledBuf := m.getBuffer()

		n, err := s.Conn.Read(pooledBuf.Data)
		if n > 0 {
			pooledBuf.Length = n
			m.router.RouteData(s.ID, pooledBuf)
		} else {
			pooledBuf.Release()
		}

		if err != nil {
			m.router.HandleClosedStream(s.ID)
			return
		}
	}
}

func (m *Merger) writer() {
	defer close(m.OutputStream.Closed)
	defer m.OutputStream.Conn.Close()

	log.Printf("started writing to output-stream")

	conn := m.OutputStream.Conn

	for {
		chunk, ok := <-m.OutputStream.Ch
		if !ok {
			return
		}

		if m.opts.WriteTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(m.opts.WriteTimeout))
		}

		_, err := conn.Write(chunk)
		if err != nil {
			log.Printf("error occurred in writer: %v", err)
			return
		}
	}
}

func (m *Merger) CreateInstance(counter int, streams map[string]*router.Stream) *MergeInstance {
	instanceID := fmt.Sprintf("merger-%d", counter)

	ctx, cancel := context.WithCancel(context.Background())

	inputChans := make([]chan []byte, 0, len(streams))
	producerIDs := make([]string, 0, len(streams))

	for id, stream := range streams {
		ch := stream.Ch
		inputChans = append(inputChans, ch)
		producerIDs = append(producerIDs, id)
	}

	return NewMergeInstance(instanceID, ctx, cancel, inputChans, producerIDs)
}

func (m *Merger) snapshotProducers() map[string]*router.Stream {
	m.inputsMu.Lock()
	defer m.inputsMu.Unlock()

	snapshotProducers := make(map[string]*router.Stream, len(m.inputs))
	for id, stream := range m.inputs {
		snapshotProducers[id] = stream
	}
	return snapshotProducers
}

func (m *Merger) getBuffer() *utils.PooledBuffer {
	return m.bufferPool.Get().(*utils.PooledBuffer)
}
