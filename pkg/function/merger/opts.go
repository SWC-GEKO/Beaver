package main

import (
	"platform/pkg/function/utils"
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
	utils.TCPOpts

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
		TCPOpts:               utils.DefaultTCPOpts(),
	}
}
