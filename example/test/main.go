package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/herryg91/gobatch"
)

type LiveEventUser struct {
	Serial     string
	UserSerial string
}

func main() {
	batchSize := 1000
	maxWaitTime := time.Second * 2
	b := gobatch.NewMemoryBatch(doBatch, batchSize, maxWaitTime, 1)

	for i := 0; i < 100200; i++ {
		b.Insert(LiveEventUser{
			Serial:     "serial-" + strconv.Itoa(i),
			UserSerial: "userSerial-" + strconv.Itoa(i),
		})
	}

	fmt.Println("Stopping")
	time.Sleep(time.Second * 10)
}

func doBatch(workerID int, datas []interface{}) (err error) {
	//do something
	liveEventUserDatas := []LiveEventUser{}
	for _, data := range datas {
		if parsedValue, ok := data.(LiveEventUser); ok {
			liveEventUserDatas = append(liveEventUserDatas, parsedValue)
		}
	}
	if len(liveEventUserDatas) > 0 {
		joinEventBatch(liveEventUserDatas)
	}
	return
}

func joinEventBatch(datas []LiveEventUser) {
	fmt.Println("INSERT INTO COCKROACH (JOIN USER):", len(datas), "datas")
	//logic to insert to cockroach
}
