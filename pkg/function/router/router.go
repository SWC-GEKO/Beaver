package router

import (
	"platform/pkg/function/utils"
	"sync"
)

type Routable interface {
	ShutdownInstance()
	Close(streamID string)
	GetStreamIndex(streamID string) (int, bool)
	RouteData(streamIndex int, chunk *utils.PooledBuffer)
}

type Router[T Routable] struct {
	Mu        sync.RWMutex
	Instances []*T
}

func NewRouter[T Routable]() *Router[T] {
	return &Router[T]{
		Instances: make([]*T, 0),
	}
}

func (r *Router[T]) RemoveInstance(instance *T) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	for i, existing := range r.Instances {
		if existing == instance {
			r.Instances = append(r.Instances[:i], r.Instances[i+1:]...)
			(*instance).ShutdownInstance()
			break
		}
	}
}

func (r *Router[T]) HandleClosedStream(streamID string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	for _, instance := range r.Instances {
		(*instance).Close(streamID)
	}
}

func (r *Router[T]) RouteData(streamID string, chunk *utils.PooledBuffer) {
	r.Mu.Lock()
	defer r.Mu.Unlock()

	for _, i := range r.Instances {
		instance := *i
		idx, ok := instance.GetStreamIndex(streamID)
		if !ok {
			continue
		}

		instance.RouteData(idx, chunk)
	}

}
