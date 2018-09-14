package main

import (
	"log"
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

	wg := sync.WaitGroup{}
	wg.Add(3)
	totalClap := 2000000

	go func() {
		defer wg.Done()
		log.Println("Start NonBatch")
		nonBatch := NewNonBatchBenchmark(rPool)
		startLogging := time.Now()
		for i := 0; i < totalClap; i++ {
			articleID := generateClap()
			nonBatch.Clap(articleID)
		}
		log.Println("NonBatch done in:", time.Since(startLogging).Seconds(), "s")
	}()

	go func() {
		defer wg.Done()
		log.Println("Start MemBatch 50.000")
		memBatch := NewMemBatchBenchmark(rPool, 50000, 1)
		memBatch.SetDoneCriteria(totalClap)
		startLogging := time.Now()
		for i := 0; i < totalClap; i++ {
			articleID := generateClap()
			memBatch.Clap(articleID)
		}
		<-memBatch.batchProcessDone
		log.Println("MemBatch 50.000 done in:", time.Since(startLogging).Seconds(), "s")
	}()

	go func() {
		defer wg.Done()
		log.Println("Start MemBatch 1.000.000")
		memBatch := NewMemBatchBenchmark(rPool, 1000000, 1)
		memBatch.SetDoneCriteria(totalClap)
		startLogging := time.Now()
		for i := 0; i < totalClap; i++ {
			articleID := generateClap()
			memBatch.Clap(articleID)
		}
		<-memBatch.batchProcessDone
		log.Println("MemBatch 1.000.000 done in:", time.Since(startLogging).Seconds(), "s")
	}()
	wg.Wait()
}

func generateClap() (articleID int) {
	articleID = int(time.Now().UnixNano() % 10000)
	return
}
