package main

import (
	"log"
	"time"

	"github.com/herryg91/hgbatch"
)

func fn1(datas []interface{}) (err error) {
	log.Println("fn1: processing", len(datas), "datas")
	return
}

func main() {
	mBatch := batch.NewMemoryBatch(
		fn1,
		100,
		time.Second*15,
		2,
	)

	for i := 0; i < 350; i++ {
		err := mBatch.Insert(i)
		if err != nil {
			log.Println(err)
		}
		time.Sleep(time.Millisecond * 20)
	}

	time.Sleep(time.Minute * 30)
}
