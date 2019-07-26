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
	maxWaitTime := time.Second * 3
	b := gobatch.NewMemoryBatch(batchSize, maxWaitTime, doBatch, 2)
	go func() {
		for i := 0; i < 30200; i++ {
			b.Insert(LiveEventUser{
				Serial:     "serial-" + strconv.Itoa(i),
				UserSerial: "userSerial-" + strconv.Itoa(i),
			})
		}
	}()
	go func() {
		for i := 0; i < 30200; i++ {
			b.Insert(LiveEventUser{
				Serial:     "serial-" + strconv.Itoa(i),
				UserSerial: "userSerial-" + strconv.Itoa(i),
			})
		}
	}()
	go func() {
		for i := 0; i < 30200; i++ {
			b.Insert(LiveEventUser{
				Serial:     "serial-" + strconv.Itoa(i),
				UserSerial: "userSerial-" + strconv.Itoa(i),
			})
		}
	}()

	fmt.Println("Stopping")
	time.Sleep(time.Second * 60)
}

func doBatch(workerID int, datas []interface{}) {
	//do something
	liveEventUserDatas := []LiveEventUser{}
	for _, data := range datas {
		if parsedValue, ok := data.(LiveEventUser); ok {
			liveEventUserDatas = append(liveEventUserDatas, parsedValue)
		}
	}
	if len(liveEventUserDatas) > 0 {
		joinEventBatch(workerID, liveEventUserDatas)
	}
	return
}

func joinEventBatch(workerID int, datas []LiveEventUser) {
	fmt.Println("INSERT INTO (JOIN USER):", len(datas), "DATAS, BY WORKER", workerID)
	time.Sleep(time.Millisecond * 3500)
}
