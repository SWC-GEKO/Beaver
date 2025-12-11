package utils

import (
	"log"
	"net"
	"time"
)

type TCPOpts struct {
	TCPNoDelay        bool
	TCPReadBuffer     int
	TCPWriteBuffer    int
	TCPKeepAlive      bool
	TCPKeepAliveIdle  time.Duration
	TCPKeepAliveIntvl time.Duration
}

func DefaultTCPOpts() TCPOpts {
	return TCPOpts{
		TCPNoDelay:        true,
		TCPReadBuffer:     512 * 1024,
		TCPWriteBuffer:    512 * 1024,
		TCPKeepAlive:      true,
		TCPKeepAliveIdle:  30 * time.Second,
		TCPKeepAliveIntvl: 10 * time.Second,
	}
}

func (opts *TCPOpts) TuneTCP(conn net.Conn) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	if opts.TCPNoDelay {
		err := tcp.SetNoDelay(true)
		if err != nil {
			log.Printf("failed to set tcp-conn: %v to no delay: %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if opts.TCPReadBuffer > 0 {
		err := tcp.SetReadBuffer(opts.TCPReadBuffer)
		if err != nil {
			log.Printf("failed to set read-buffer size for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if opts.TCPWriteBuffer > 0 {
		err := tcp.SetWriteBuffer(opts.TCPWriteBuffer)
		if err != nil {
			log.Printf("failed to set write-buffer size for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
			return
		}
	}

	if !opts.TCPKeepAlive {
		return
	}

	keepAliveOpts := net.KeepAliveConfig{
		Enable:   opts.TCPKeepAlive,
		Idle:     opts.TCPKeepAliveIdle,
		Interval: opts.TCPKeepAliveIntvl,
	}

	err := tcp.SetKeepAliveConfig(keepAliveOpts)
	if err != nil {
		log.Printf("failed to set keep alive config for tcp-conn: %v, %v", tcp.RemoteAddr(), err)
		return
	}
}
