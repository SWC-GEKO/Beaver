package main

import (
	"net"
	"time"
)

type SplitterOpts struct {
	InputListenerAddr      string
	FunctionName           string
	InitialOutputFunctions int
	MaxOutputFunctions     int
	AcceptTimeout          time.Duration

	ChunkSize         int
	ChannelBufferSize int
	InputBufferCap    int // idk (because we would like to tigger scale out if this overflows)

	TCPNoDelay        bool
	TCPReadBuffer     int
	TCPWriteBuffer    int
	TCPKeepAlive      bool
	TCPKeepAliveIdle  time.Duration
	TCPKeepAliveIntvl time.Duration
}

func DefaultOps() *SplitterOpts {
	return &SplitterOpts{
		InputListenerAddr:      ":8000",
		FunctionName:           "processor", // used to discover the functions
		InitialOutputFunctions: 5,
		MaxOutputFunctions:     10,
		AcceptTimeout:          5 * time.Second,
		ChunkSize:              32 * 1024,
		ChannelBufferSize:      256,
		InputBufferCap:         512 * 1024,
		TCPNoDelay:             true,
		TCPReadBuffer:          512 * 1024,
		TCPWriteBuffer:         512 * 1024,
		TCPKeepAlive:           true,
		TCPKeepAliveIdle:       30 * time.Second,
		TCPKeepAliveIntvl:      10 * time.Second,
	}
}

type InputStream struct {
	conn   net.Conn
	Ch     chan []byte
	closed chan struct{}
}

type OutputStream struct {
	ID       string
	conn     net.Conn
	Ch       chan []byte
	bytesOut int64
	closed   chan struct{}
}

func NewInputStream(conn net.Conn, chanSize int) *InputStream {
	return &InputStream{
		conn:   conn,
		Ch:     make(chan []byte, chanSize),
		closed: make(chan struct{}),
	}
}

func NewOutputStream(id string, conn net.Conn, chanSize int) *OutputStream {
	return &OutputStream{
		ID:     id,
		conn:   conn,
		Ch:     make(chan []byte, chanSize),
		closed: make(chan struct{}),
	}
}
