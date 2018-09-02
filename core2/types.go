package core2

import "sync"

type Batch interface {
	Insert(interface{}) error
	Inserts([]interface{}) error

	flush([]interface{})
	flushWorker(int, <-chan []interface{})
	setupFlushWorker(int)
}

type FlushConfig struct {
	MaxData int
}

type MemoryBatch struct {
	items     []interface{}
	mutex     *sync.RWMutex
	fHandlers []BufferHandlerFunc

	flushCfg FlushConfig

	jobs chan []interface{}
}

type BufferHandlerFunc func([]interface{}) error
