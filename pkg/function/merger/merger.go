package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type MergeInstance struct {
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	inputChans  []chan []byte
	producerIDs []string
}

type StreamRouter struct {
	mu        sync.RWMutex
	instances []*MergeInstance
}

type Merger struct {
	opts *MergerOpts

	MergeFunction func(context.Context, []chan []byte, chan []byte) error
	inputsMu      sync.RWMutex
	readyInit     sync.Once
	readyCh       chan struct{}
	inputs        map[string]*InputStream
	acceptSem     chan struct{}
	OutputStream  *OutputStream
	router        *StreamRouter

	metricsTicker *time.Ticker
	lastLogTime   time.Time
}

func NewMerger(opts *MergerOpts, mergeFunction func(context.Context, []chan []byte, chan []byte) error) *Merger {

	inputs := make(map[string]*InputStream)
	acceptSem := make(chan struct{}, opts.MaxInputFunctions)

	return &Merger{
		opts:          opts,
		MergeFunction: mergeFunction,
		inputs:        inputs,
		acceptSem:     acceptSem,
		readyCh:       make(chan struct{}),
	}
}

func (m *Merger) Run() error {
	log.Printf("started running merger")
	out, err := net.Dial("tcp", m.opts.DownstreamAddr)
	if err != nil {
		return err
	}
	m.tuneTCP(out)

	m.OutputStream = NewOutputStream(out, m.opts.ChannelBufferSize)

	go m.writer()
	go func() {
		defer out.Close()
		<-m.OutputStream.closed
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
		log.Printf("starting merge function with inputs: %v and output: %v", m.inputs, m.OutputStream)
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
	m.router = &StreamRouter{
		instances: make([]*MergeInstance, 0),
	}

	instanceCounter := 0

	for {
		m.inputsMu.Lock()
		snapshotProducers := make(map[string]*InputStream, len(m.inputs))

		for id, stream := range m.inputs {
			snapshotProducers[id] = stream
		}
		currentLen := len(m.inputs)
		m.inputsMu.Unlock()

		if currentLen == 0 {
			log.Printf("no inputs available")
			time.Sleep(100 * time.Millisecond) // Change this, maybe to have a wake-up call
			continue
		}

		instanceID := fmt.Sprintf("merger-%d", instanceCounter)
		instanceCounter++

		ctx, cancel := context.WithCancel(context.Background())

		inputChans := make([]chan []byte, 0, len(snapshotProducers))
		producerIDs := make([]string, 0, len(snapshotProducers))

		for id, stream := range snapshotProducers {
			ch := stream.Ch
			inputChans = append(inputChans, ch)
			producerIDs = append(producerIDs, id)
		}

		instance := &MergeInstance{
			id:          instanceID,
			ctx:         ctx,
			cancel:      cancel,
			inputChans:  inputChans,
			producerIDs: producerIDs,
		}

		m.router.mu.Lock()
		m.router.instances = append(m.router.instances, instance)
		m.router.mu.Unlock()

		log.Printf("starting merge instance: %s with %d inputs: %v", instanceID, len(producerIDs), producerIDs)

		go func(inst *MergeInstance) {
			defer func() {
				m.router.mu.Lock()
				for i, existing := range m.router.instances {
					if existing.id == inst.id {
						m.router.instances = append(m.router.instances[:i], m.router.instances[i+1:]...)
						break
					}
				}

				m.router.mu.Unlock()

				for _, ch := range inst.inputChans {
					close(ch)
				}

				log.Printf("merge instance %s cleaned up", inst.id)
			}()

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

				if newLen != currentLen {
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

	stream := NewInputStream(id, conn, m.opts.ChannelBufferSize)

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

	<-stream.closed

	m.inputsMu.Lock()
	delete(m.inputs, id)
	m.inputsMu.Unlock()
}

func (m *Merger) reader(s *InputStream) {
	defer close(s.closed)

	log.Printf("starting to read from incoming conn: %v", s.ID)
	buf := make([]byte, m.opts.ChunkSize)

	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			//ID = RemoteAddr of the Conn
			m.routeData(s.ID, chunk)
		}

		if err != nil {
			m.producerClosed(s.ID)
			return
		}
	}
}

func (m *Merger) writer() {
	defer close(m.OutputStream.closed)
	defer m.OutputStream.conn.Close()

	log.Printf("started writing to output-stream")

	conn := m.OutputStream.conn
	buf := make([]byte, m.opts.ChunkSize)

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}

		if m.opts.WriteTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(m.opts.WriteTimeout))
		}

		log.Printf("flushing buffer with length: %d", len(buf))
		_, err := conn.Write(buf)
		buf = buf[:0]
		return err
	}

	for {
		select {
		case chunk := <-m.OutputStream.Ch:
			log.Printf("fetched chunk with length: %d from output-chan", len(chunk))
			if len(buf)+len(chunk) > cap(buf) {
				if err := flush(); err != nil {
					return
				}
			}

			buf = append(buf, chunk...)

		case <-time.After(m.opts.WriteFlushInterval):
			if err := flush(); err != nil {
				return
			}
		}
	}
}

func (m *Merger) routeData(producerID string, chunk []byte) {
	m.router.mu.RLock()
	defer m.router.mu.RUnlock()

	for _, instance := range m.router.instances {
		producerIndex := -1
		for i, id := range instance.producerIDs {
			if id == producerID {
				producerIndex = i
				break
			}
		}

		if producerIndex == -1 {
			continue
		}

		select {
		case <-instance.ctx.Done():
			continue
		case instance.inputChans[producerIndex] <- chunk:
		default:
			select {
			case <-instance.ctx.Done():
				continue
			case instance.inputChans[producerIndex] <- chunk:
				log.Printf("successfully sent after blocking")
			case <-time.After(5 * time.Second):
				log.Printf("ERROR: dropping data after timeout for %s", producerID)
			}
		}
	}
}

func (m *Merger) producerClosed(producerID string) {
	m.router.mu.RLock()
	defer m.router.mu.RUnlock()

	for _, instance := range m.router.instances {
		producerIndex := -1
		for i, id := range instance.producerIDs {
			if id == producerID {
				producerIndex = i
				break
			}
		}

		if producerIndex >= 0 {
			select {
			case <-instance.ctx.Done():
				log.Printf("instance is already closing")
			default:
				close(instance.inputChans[producerIndex])
				log.Printf("closed input chan: %d in instance: %s for producer: %s", producerIndex, instance.id, producerID)
			}
		}
	}
}

func (m *Merger) tuneTCP(conn net.Conn) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	if m.opts.TCPNoDelay {
		err := tcp.SetNoDelay(true)
		if err != nil {
			log.Printf("failed to set tcp-conn: %v to no delay: %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if m.opts.TCPReadBuffer > 0 {
		err := tcp.SetReadBuffer(m.opts.TCPReadBuffer)
		if err != nil {
			log.Printf("failed to set read-buffer size for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if m.opts.TCPWriteBuffer > 0 {
		err := tcp.SetWriteBuffer(m.opts.TCPWriteBuffer)
		if err != nil {
			log.Printf("failed to set write-buffer size for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if !m.opts.TCPKeepAlive {
		return
	}

	keepAliveOpts := net.KeepAliveConfig{
		Enable:   m.opts.TCPKeepAlive,
		Idle:     m.opts.TCPKeepAliveIdle,
		Interval: m.opts.TCPKeepAliveIntvl,
	}

	err := tcp.SetKeepAliveConfig(keepAliveOpts)
	if err != nil {
		log.Printf("failed to set keep alive config for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
		return
	}
}
