package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/herryg91/gobatch"
)

type MemBatchCfg struct {
	RedisPool        *redis.Pool
	MemBatchInstance gobatch.Batch

	locker            *sync.Mutex
	batchProcessed    int
	maxBatchProcessed int
	batchProcessDone  chan bool
}

func NewMemBatchBenchmark(redisPool *redis.Pool, batchSize int, batchWorker int) *MemBatchCfg {
	instance := &MemBatchCfg{
		RedisPool: redisPool,

		locker:            &sync.Mutex{},
		batchProcessed:    0,
		maxBatchProcessed: 1000000,
		batchProcessDone:  make(chan bool),
	}
	instance.MemBatchInstance = gobatch.NewMemoryBatch(instance.addClapToDB, batchSize, time.Second*15, batchWorker)
	return instance
}
func (b *MemBatchCfg) SetDoneCriteria(maxBatchProcessed int) {
	b.maxBatchProcessed = maxBatchProcessed
}

func (b *MemBatchCfg) Clap(articleID int) {
	b.MemBatchInstance.Insert(articleID)
}

func (b *MemBatchCfg) addClapToDB(workerID int, datas []interface{}) (err error) {
	mapOfClaps := map[int]int{} //key = article id, value = number of clap
	for _, d := range datas {
		if articleID, okParse := d.(int); okParse {
			if _, okCheckMap := mapOfClaps[articleID]; !okCheckMap {
				mapOfClaps[articleID] = 0
			}
			mapOfClaps[articleID]++
		}
	}

	for articleID, score := range mapOfClaps {
		rdsConn := b.RedisPool.Get()
		rdsConn.Do("ZINCRBY", "benchmark:membatch", score, strconv.Itoa(articleID))
		rdsConn.Close()
	}

	b.locker.Lock()
	b.batchProcessed += len(datas)
	if b.batchProcessed >= b.maxBatchProcessed {
		b.batchProcessDone <- true
	}
	b.locker.Unlock()
	return
}
