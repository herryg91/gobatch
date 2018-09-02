package core2

import (
	"sync"
	"time"
)

type Batch interface {
	Insert(interface{}) error
	Inserts([]interface{}) error

	flush([]interface{})
	flushWorker(int, <-chan []interface{})
	setupFlushWorker(int)
}

type FlushConfig struct {
	maxSize int
	maxWait time.Duration
}

type MemoryBatch struct {
	items []interface{}
	mutex *sync.RWMutex
	doFn  BufferHandlerFunc

	flushCfg FlushConfig

	jobs  chan []interface{}
	add   chan interface{}
	close chan bool
}

type BufferHandlerFunc func([]interface{}) error
