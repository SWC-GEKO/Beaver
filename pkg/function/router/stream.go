package router

import "net"

type Stream struct {
	ID            string
	Conn          net.Conn
	Ch            chan []byte
	StreamedBytes int64
	Closed        chan struct{}
}

func NewStream(id string, conn net.Conn, chanSize int) *Stream {
	return &Stream{
		ID:     id,
		Conn:   conn,
		Ch:     make(chan []byte, chanSize),
		Closed: make(chan struct{}),
	}
}
