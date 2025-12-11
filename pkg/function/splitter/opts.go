package main

import (
	"platform/pkg/function/utils"
	"time"
)

type SplitterOpts struct {
	InputListenerAddr      string
	FunctionName           string
	FunctionPort           string
	InitialOutputFunctions int
	MaxOutputFunctions     int
	AcceptTimeout          time.Duration
	WriterTimeout          time.Duration
	ReaderTimeout          time.Duration

	ChunkSize         int
	ChannelBufferSize int
	InputBufferCap    int // idk (because we would like to tigger scale out if this overflows)

	utils.TCPOpts
}

func DefaultOps() *SplitterOpts {
	return &SplitterOpts{
		InputListenerAddr:      ":8000",
		FunctionName:           "processor", // used to discover the functions
		FunctionPort:           ":8000",
		InitialOutputFunctions: 5,
		MaxOutputFunctions:     10,
		AcceptTimeout:          5 * time.Second,
		ChunkSize:              32 * 1024,
		ChannelBufferSize:      256,
		InputBufferCap:         512 * 1024,
		TCPOpts:                utils.DefaultTCPOpts(),
	}
}
