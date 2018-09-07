package batch

import (
	"log"
	"sync"
	"time"
)

type MemoryBatch struct {
	items []interface{}
	mutex *sync.RWMutex
	doFn  BufferDoFn

	maxSize int
	maxWait time.Duration

	flushJobs  chan []interface{}
	insertChan chan interface{}
	flushChan  chan interface{}
	stopChan   chan bool
}

func NewMemoryBatch(flushHandler BufferDoFn, flushMaxSize int, flushMaxWait time.Duration, workerSize int) Batch {
	instance := &MemoryBatch{
		items: []interface{}{},
		doFn:  flushHandler,
		mutex: &sync.RWMutex{},

		maxSize: flushMaxSize,
		maxWait: flushMaxWait,

		flushJobs:  make(chan []interface{}, workerSize),
		flushChan:  make(chan interface{}),
		insertChan: make(chan interface{}),
		stopChan:   make(chan bool),
	}

	instance.runWorker(workerSize)
	go instance.runBatch()
	return instance
}

func (i *MemoryBatch) runWorker(workerSize int) {
	if workerSize < 1 {
		workerSize = 1
	}
	for id := 1; id <= workerSize; id++ {
		go func(workerID int, flushJobs <-chan []interface{}) {
			for j := range flushJobs {
				i.flush(workerID, j)
			}
		}(id, i.flushJobs)
	}
}

func (i *MemoryBatch) flush(workerID int, datas []interface{}) {
	err := i.doFn(workerID, datas)
	if err != nil {
		log.Println("[error]", err)
	}
	return
}

func (i *MemoryBatch) Insert(data interface{}) (err error) {
	i.insertChan <- data
	return
}
func (i *MemoryBatch) Flush() (err error) {
	i.flushChan <- true
	return
}
func (i *MemoryBatch) Stop() {
	i.stopChan <- true
}

func (i *MemoryBatch) runBatch() {
	for {
		select {
		case <-time.Tick(i.maxWait):
			if len(i.items) == 0 {
				break
			}

			i.mutex.Lock()
			// Write batch contents to channel,
			i.flushJobs <- i.items
			i.items = i.items[:0]
			i.mutex.Unlock()
		case item := <-i.insertChan:
			i.mutex.Lock()
			i.items = append(i.items, item)
			if len(i.items) >= i.maxSize {
				i.flushJobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
		case <-i.flushChan:
			i.mutex.Lock()
			if len(i.items) > 0 {
				i.flushJobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
		case isStop := <-i.stopChan:
			if isStop {
				return
			}
		}
	}
}
