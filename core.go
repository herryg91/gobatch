package batch

type Batch interface {
	flush(workerID int, datas []interface{})
	setFlushWorker(workerSize int)

	Insert(data interface{}) (err error)
	ForceFlush() (err error)
	Stop() (err error)
}

type BufferDoFn func(workerID int, datas []interface{}) (err error)
