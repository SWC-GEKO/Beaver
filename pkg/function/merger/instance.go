package main

import (
	"context"
	"log"
	"platform/pkg/function/utils"
	"slices"
	"time"
)

type MergeInstance struct {
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	inputChans  []chan []byte
	producerIDs []string
}

func NewMergeInstance(id string, ctx context.Context, cancel context.CancelFunc, inputChans []chan []byte, producerIDs []string) *MergeInstance {
	return &MergeInstance{
		id:          id,
		ctx:         ctx,
		cancel:      cancel,
		inputChans:  inputChans,
		producerIDs: producerIDs,
	}
}

func (m *MergeInstance) GetStreamIndex(streamID string) (int, bool) {
	if !slices.Contains(m.producerIDs, streamID) {
		return -1, false
	}

	return slices.Index(m.producerIDs, streamID), true
}

func (m *MergeInstance) ShutdownInstance() {
	for _, ch := range m.inputChans {
		close(ch)
	}
}

func (m *MergeInstance) RouteData(streamIndex int, chunk *utils.PooledBuffer) {
	defer chunk.Release()

	select {
	case <-m.ctx.Done():
		return
	case m.inputChans[streamIndex] <- chunk.Bytes():
	case <-time.After(5 * time.Second): // TODO: implement this dynamically
		log.Printf("dropping data after timeout for %s", m.producerIDs[streamIndex])
	}
}

func (m *MergeInstance) Close(streamID string) {
	idx, ok := m.GetStreamIndex(streamID)
	if !ok {
		return
	}

	select {
	case <-m.ctx.Done():
		log.Printf("Producer [%s] is already done", m.producerIDs[idx])
	default:
		close(m.inputChans[idx])
	}
}
