package core2

import "sync"

// type ElementProcessor interface {
// 	// Call processes a single element. If GBK or CoGBK result, the values
// 	// are populated. Otherwise, they're empty.
// 	ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error
// }

type Batch interface {
	// Do([]interface{}) error
	Insert(interface{}) error
	Inserts([]interface{}) error

	flush([]interface{})
	flushWorker(int, <-chan []interface{})
	setupFlushWorker(int)
}

// type BatchConfig struct {
// 	maxData   int
// 	mutex     *sync.RWMutex
// 	fHandlers BufferHandlerFunc
// }

type FlushConfig struct {
	MaxData int
}

type MemoryBatch struct {
	items     []interface{}
	mutex     *sync.RWMutex
	fHandlers []BufferHandlerFunc

	flushCfg FlushConfig

	jobs chan []interface{}
	// totalWorker int
	// jobs        chan []interface{}

}

// type (
// 	Instance struct {
// 		MaxData   int
// 		mutex     *sync.RWMutex
// 		datas     []interface{}
// 		fHandlers BufferHandlerFunc

// 		totalWorker int
// 		jobs        chan []interface{}

// 		batchType batchtype
// 	}
// )

type BufferHandlerFunc func([]interface{}) error
