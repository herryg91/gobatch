package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/herryg91/gobatch"
)

type BenchmarkCfg struct {
	MaxSize   int
	RedisPool *redis.Pool

	locker           *sync.Mutex
	batchProcessed   int
	batchProcessDone chan bool
}

func (b *BenchmarkCfg) bfn1(workerID int, datas []interface{}) (err error) {
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
func (b *BenchmarkCfg) benchmarkBatch() {
	log.Println("start benchmarkBatch")
	startLogging := time.Now()
	mBatch := gobatch.NewMemoryBatch(b.bfn1, 100, time.Second*15, 1)
	for i := 1; i <= b.MaxSize; i++ {
		mBatch.Insert(i)
	}

	<-b.batchProcessDone
	log.Println("benchmarkBatch done in:", time.Since(startLogging).Seconds(), "s")

}
func (b *BenchmarkCfg) benchmarkNonBatch() {
	log.Println("start benchmarkNonBatch")
	startLogging := time.Now()
	for i := 1; i <= b.MaxSize; i++ {
		rdsConn := b.RedisPool.Get()
		rdsConn.Do("set", fmt.Sprintf("benchmark:non:%d", i), strconv.Itoa(i))
		rdsConn.Close()
	}
	log.Println("benchmarkNonBatch done in:", time.Since(startLogging).Seconds(), "s")
}
