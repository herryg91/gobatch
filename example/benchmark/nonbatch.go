package main

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

const nonBatchKey = "benchmark:nonbatch"

type NonBatchCfg struct {
	RedisPool *redis.Pool
}

func NewNonBatchBenchmark(redisPool *redis.Pool) *NonBatchCfg {
	return &NonBatchCfg{
		RedisPool: redisPool,
	}
}

func (b *NonBatchCfg) Clap(articleID int) {
	rdsConn := b.RedisPool.Get()
	rdsConn.Do("ZINCRBY", nonBatchKey, 1, strconv.Itoa(articleID))
	rdsConn.Close()
}
