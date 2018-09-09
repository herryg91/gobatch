# hgbatch

Simple batch library for Golang.

## How to Use

Install:

```
go get github.com/herryg91/gobatch
```

Example:

```
func fn1(workerID int, datas []interface{}) (err error) {
    //do something
    return
}

// every 100 datas or 15 second no activity, batch will be processed (fn1 will be run) with 2 worker
mBatch := batch.NewMemoryBatch(fn1, 100, time.Second*15, 2)

mBatch.Insert(interface{}{})
mBatch.Insert(interface{}{})
mBatch.Insert(interface{}{})
```

## Future Update:
- Batch using Redis
- Batch using File
- Add / Decrease Worker on the fly
