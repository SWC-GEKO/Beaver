package utils

import "sync"

type PooledBuffer struct {
	Data   []byte
	Length int
	Pool   *sync.Pool
}

func (pb *PooledBuffer) Bytes() []byte {
	return pb.Data[:pb.Length]
}

func (pb *PooledBuffer) Release() {
	pb.Pool.Put(pb)
}
