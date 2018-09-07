package batch

type Batch interface {
	flush(workerID int, datas []interface{})
	runWorker(workerSize int)
	runBatch()

	Insert(data interface{}) (err error)
	Flush() (err error)
	Stop()
}

type BufferDoFn func(workerID int, datas []interface{}) (err error)
