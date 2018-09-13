package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/herryg91/gobatch"
)

type MemBatchCfg struct {
	MaxSize   int
	RedisPool *redis.Pool

	locker           *sync.Mutex
	batchProcessed   int
	batchProcessDone chan bool
}

func (b *MemBatchCfg) bfn1(workerID int, datas []interface{}) (err error) {
	values := []interface{}{}
	for _, d := range datas {
		values = append(values, fmt.Sprintf("test:%v", d), fmt.Sprintf("%v", d))
	}
	rdsConn := b.RedisPool.Get()
	defer rdsConn.Close()
	rdsConn.Do("mset", values...)

	b.locker.Lock()
	b.batchProcessed += len(datas)
	if b.batchProcessed >= b.MaxSize {
		b.locker.Unlock()
		b.batchProcessDone <- true
		return
	}
	b.locker.Unlock()
	return
}
func (b *MemBatchCfg) Run() {
	log.Println("Start MemBatch")
	b.batchProcessed = 0
	b.batchProcessDone = make(chan bool, 1)
	b.locker = &sync.Mutex{}

	startLogging := time.Now()
	mBatch := gobatch.NewMemoryBatch(b.bfn1, 100, time.Second*15, 1)
	for i := 1; i <= b.MaxSize; i++ {
		mBatch.Insert(i)
	}

	<-b.batchProcessDone
	log.Println("MemBatch done in:", time.Since(startLogging).Seconds(), "s")

}
