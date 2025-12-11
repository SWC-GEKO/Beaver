package main

import (
	"context"
	"log"
	"platform/pkg/function/utils"
	"slices"
	"time"
)

type SplitInstance struct {
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	outputChans []chan []byte
	consumerIDs []string
}

func NewSplitInstance(id string, ctx context.Context, cancel context.CancelFunc, outputs []chan []byte, consumerIDs []string) *SplitInstance {
	return &SplitInstance{
		id:          id,
		ctx:         ctx,
		cancel:      cancel,
		outputChans: outputs,
		consumerIDs: consumerIDs,
	}
}

func (s *SplitInstance) ShutdownInstance() {
	for _, ch := range s.outputChans {
		close(ch)
	}
}

func (s *SplitInstance) Close(streamID string) {
	idx, ok := s.GetStreamIndex(streamID)
	if !ok {
		return
	}

	select {
	case <-s.ctx.Done():
		log.Printf("Consumer [%s] is already done", s.consumerIDs[idx])
	default:
		close(s.outputChans[idx])
	}
}

func (s *SplitInstance) GetStreamIndex(streamID string) (int, bool) {
	if !slices.Contains(s.consumerIDs, streamID) {
		return -1, false
	}

	return slices.Index(s.consumerIDs, streamID), true
}

func (s *SplitInstance) RouteData(streamIndex int, chunk *utils.PooledBuffer) {
	defer chunk.Release()

	select {
	case <-s.ctx.Done():
		return
	case s.outputChans[streamIndex] <- chunk.Bytes():
	case <-time.After(5 * time.Second):
		log.Printf("dropping data after timeout for %s", s.consumerIDs[streamIndex])
	}
}
