package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

type NonBatchCfg struct {
	MaxSize   int
	RedisPool *redis.Pool
}

func (b *NonBatchCfg) Run() {
	log.Println("Start NonBatch")
	startLogging := time.Now()
	for i := 1; i <= b.MaxSize; i++ {
		rdsConn := b.RedisPool.Get()
		rdsConn.Do("set", fmt.Sprintf("benchmark:nonbatch:%d", i), strconv.Itoa(i))
		rdsConn.Close()
	}
	log.Println("NonBatch done in:", time.Since(startLogging).Seconds(), "s")
}
