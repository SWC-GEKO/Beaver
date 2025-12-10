package main

import (
	"net"
	"time"
)

type MergerOpts struct {
	InputListenerAddr     string
	DownstreamAddr        string
	InitialInputFunctions int
	MaxInputFunctions     int
	AcceptTimeout         time.Duration

	ChunkSize         int
	ChannelBufferSize int
	PerInputBufferCap int

	// 1:1 will be transported to TCP-Conn
	TCPNoDelay        bool
	TCPReadBuffer     int
	TCPWriteBuffer    int
	TCPKeepAlive      bool
	TCPKeepAliveIdle  time.Duration
	TCPKeepAliveIntvl time.Duration

	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	WriteFlushInterval time.Duration

	EnableBufferPool bool
	PoolBufferSize   int
	PoolMaxBuffers   int

	MetricsEnabled  bool
	MetricsInterval time.Duration
	LogRateLimit    time.Duration
	LogLevel        string
}

func DefaultOpts(
	downstreamAddr string,
	maxInputFunctions int,
) *MergerOpts {
	return &MergerOpts{
		InputListenerAddr:     ":8000",
		DownstreamAddr:        downstreamAddr,
		MaxInputFunctions:     maxInputFunctions,
		InitialInputFunctions: 5,
		AcceptTimeout:         5 * time.Second,
		ChunkSize:             64 * 1024,
		ChannelBufferSize:     256,
		PerInputBufferCap:     256 * 1024,
		TCPNoDelay:            true,
		TCPReadBuffer:         512 * 1024,
		TCPWriteBuffer:        512 * 1024,
		TCPKeepAlive:          true,
		TCPKeepAliveIdle:      30 * time.Second,
		TCPKeepAliveIntvl:     10 * time.Second,
		ReadTimeout:           0,
		WriteTimeout:          0,
		WriteFlushInterval:    50 * time.Millisecond,
		EnableBufferPool:      false,
		PoolBufferSize:        0,
		PoolMaxBuffers:        0,
		MetricsEnabled:        false,
		MetricsInterval:       0,
		LogRateLimit:          0,
		LogLevel:              "INFO",
	}
}

type InputStream struct {
	ID      string
	conn    net.Conn
	Ch      chan []byte
	bytesIn int64
	closed  chan struct{}
}

type OutputStream struct {
	conn   net.Conn
	Ch     chan []byte
	closed chan struct{}
}

func NewInputStream(id string, conn net.Conn, chanSize int) *InputStream {
	return &InputStream{
		ID:     id,
		conn:   conn,
		Ch:     make(chan []byte, chanSize),
		closed: make(chan struct{}),
	}
}

func NewOutputStream(conn net.Conn, chanSize int) *OutputStream {
	return &OutputStream{
		conn:   conn,
		Ch:     make(chan []byte, chanSize),
		closed: make(chan struct{}),
	}
}
