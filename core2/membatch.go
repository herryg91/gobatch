package core2

import (
	"log"
	"sync"
	"time"
)

func NewMemoryBatch(flushHandler BufferHandlerFunc, flushMaxData int, workerSize int) Batch {
	instance := &MemoryBatch{
		items: []interface{}{},
		doFn:  flushHandler,
		mutex: &sync.RWMutex{},
		flushCfg: FlushConfig{
			maxSize: flushMaxData,
			maxWait: time.Second * 10,
		},
		jobs:  make(chan []interface{}, workerSize),
		add:   make(chan interface{}),
		close: make(chan bool),
	}

	instance.setupFlushWorker(workerSize)
	go instance.run()
	return instance
}

func (i *MemoryBatch) setupFlushWorker(workerSize int) {
	for workerID := 1; workerID <= workerSize; workerID++ {
		go i.flushWorker(workerID, i.jobs)
	}
}

func (i *MemoryBatch) flush(datas []interface{}) {
	err := i.doFn(datas)
	if err != nil {
		log.Println("[error]", err)
	}
	return
}

func (i *MemoryBatch) flushWorker(workerID int, jobs <-chan []interface{}) {
	for j := range jobs {
		i.flush(j)
	}
}

func (i *MemoryBatch) Insert(data interface{}) (err error) {
	i.add <- data
	return
}
func (i *MemoryBatch) Inserts(datas []interface{}) (err error) {
	for _, data := range datas {
		i.add <- data
	}
	return
}

func (i *MemoryBatch) run() {
	for {
		select {
		// If we've reached the maximum wait time
		case <-time.Tick(i.flushCfg.maxWait):
			if len(i.items) == 0 {
				break
			}

			i.mutex.Lock()
			// Write batch contents to channel,
			i.jobs <- i.items
			i.items = i.items[:0]
			i.mutex.Unlock()
			break

		// If an item has been added to the batch.
		case item := <-i.add:
			i.mutex.Lock()
			i.items = append(i.items, item)

			// If we've reached the maximum batch size, write batch
			// contents to channel, clear batched item and add new
			// item to empty batch.
			if len(i.items) >= i.flushCfg.maxSize {
				i.jobs <- i.items
				i.items = i.items[:0]
			}
			i.mutex.Unlock()
			break

		// If the batch has been closed, wipe the batch clean,
		// close channels & exit the loop.
		case <-i.close:
			return
		}
	}
}
