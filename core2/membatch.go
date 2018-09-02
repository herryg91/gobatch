package core2

import (
	"log"
	"sync"
)

func NewMemoryBatch(flushHandlers []BufferHandlerFunc, flushMaxData int, workerSize int) Batch {
	instance := &MemoryBatch{
		items:     []interface{}{},
		fHandlers: flushHandlers,
		mutex:     &sync.RWMutex{},
		flushCfg: FlushConfig{
			MaxData: flushMaxData,
		},
		jobs: make(chan []interface{}, workerSize),
	}

	instance.setupFlushWorker(workerSize)
	return instance
}

func (i *MemoryBatch) setupFlushWorker(workerSize int) {
	for workerID := 1; workerID <= workerSize; workerID++ {
		go i.flushWorker(workerID, i.jobs)
	}
}

func (i *MemoryBatch) flush(datas []interface{}) {
	for _, fn := range i.fHandlers {
		err := fn(datas)
		if err != nil {
			log.Println("[error]", err)
		}
	}
	return
}

func (i *MemoryBatch) flushWorker(workerID int, jobs <-chan []interface{}) {
	for j := range jobs {
		i.flush(j)
	}
}

func (i *MemoryBatch) cleanDatas() {
	i.items = i.items[:0]
}
func (i *MemoryBatch) Insert(data interface{}) (err error) {
	i.mutex.Lock()
	i.items = append(i.items, data)
	if len(i.items) >= i.flushCfg.MaxData {
		//flush
		i.jobs <- i.items
		i.cleanDatas()
	}
	i.mutex.Unlock()
	return
}
func (i *MemoryBatch) Inserts(datas []interface{}) (err error) {
	i.mutex.Lock()
	i.items = append(i.items, datas...)
	if len(i.items) >= i.flushCfg.MaxData {
		//flush
		i.jobs <- i.items
		i.cleanDatas()
	}
	i.mutex.Unlock()
	return
}
