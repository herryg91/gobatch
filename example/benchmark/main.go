package main

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

func main() {
	rPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, errDial := redis.Dial("tcp", "localhost:6379")
			if errDial != nil {
				return nil, errDial
			}
			return c, nil
		},
		IdleTimeout: time.Duration(3 * time.Second),
		MaxActive:   600,
		MaxIdle:     10,
		Wait:        true,
	}

	maxSize := 1000000
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		nb := NonBatchCfg{
			MaxSize:   maxSize,
			RedisPool: rPool,
		}
		nb.Run()
	}()

	go func() {
		defer wg.Done()
		mb := MemBatchCfg{
			MaxSize:   maxSize,
			RedisPool: rPool,
		}
		mb.Run()
	}()

	wg.Wait()
}
