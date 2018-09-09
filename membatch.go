package batch

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type MemoryBatch struct {
	items []interface{}
	mutex *sync.RWMutex
	doFn  BatchDoFn

	maxSize int
	maxWait time.Duration

	flushJobs chan []interface{}
	isRun     bool

	/*notifier channel*/
	insertChan     chan interface{}
	forceFlushChan chan interface{}
	stopChan       chan bool
}

func NewMemoryBatch(flushHandler BatchDoFn, flushMaxSize int, flushMaxWait time.Duration, workerSize int) Batch {
	instance := &MemoryBatch{
		items: []interface{}{},
		doFn:  flushHandler,
		mutex: &sync.RWMutex{},

		maxSize: flushMaxSize,
		maxWait: flushMaxWait,

		flushJobs: make(chan []interface{}, workerSize),
		isRun:     false,

		forceFlushChan: make(chan interface{}),
		insertChan:     make(chan interface{}),
		stopChan:       make(chan bool),
	}

	instance.setFlushWorker(workerSize)
	instance.isRun = true
	go instance.run()
	return instance
}

/* Flush Section */
func (i *MemoryBatch) flush(workerID int, datas []interface{}) {
	err := i.doFn(workerID, datas)
	if err != nil {
		log.Println("[error]", err)
	}
	return
}

func (i *MemoryBatch) setFlushWorker(workerSize int) {
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

/* Notifier Section*/

func (i *MemoryBatch) Insert(data interface{}) (err error) {
	if i.isRun {
		i.insertChan <- data
	} else {
		err = fmt.Errorf("Failed to Insert. Batch already stopped")
	}
	return
}

func (i *MemoryBatch) ForceFlush() (err error) {
	if i.isRun {
		i.forceFlushChan <- true
	} else {
		err = fmt.Errorf("Failed to Force Flush. Batch already stopped")
	}
	return
}

func (i *MemoryBatch) Stop() (err error) {
	if i.isRun {
		i.stopChan <- true
	} else {
		err = fmt.Errorf("Failed to stop. Batch already stopped")
	}
	return
}

func (i *MemoryBatch) run() {
	for {
		select {
		case <-time.Tick(i.maxWait):
			i.mutex.Lock()
			if len(i.items) > 0 {
				i.flushJobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
		case item := <-i.insertChan:
			i.mutex.Lock()
			i.items = append(i.items, item)
			if len(i.items) >= i.maxSize {
				i.flushJobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
		case <-i.forceFlushChan:
			i.mutex.Lock()
			if len(i.items) > 0 {
				i.flushJobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
		case isStop := <-i.stopChan:
			if isStop {
				i.isRun = false
				return
			}
		}
	}
}
