package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/herryg91/gobatch"
)

func fn1(workerID int, datas []interface{}) (err error) {
	log.Println(fmt.Sprintf("worker %d: processing %d datas", workerID, len(datas)))
	return
}

func main() {
	signal_chan := make(chan os.Signal, 1)
	signal.Notify(signal_chan,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		for {
			s := <-signal_chan
			switch s {
			case syscall.SIGINT: // kill -SIGINT XXXX or Ctrl+c
				log.Println("[stop] Ctrl+C or Kill By SIGINT")
				os.Exit(0)
			case syscall.SIGTERM: // kill -SIGTERM XXXX
				log.Println("[stop] Force Stop")
				os.Exit(0)
			case syscall.SIGQUIT: // kill -SIGQUIT XXXX
				log.Println("[stop] Stop and Core Dump")
				os.Exit(0)
			default:
				log.Println("[stop] Unknown Signal")
				os.Exit(1)
			}
		}
	}()
	log.Println("Ctrl+C to Exit")

	mBatch := gobatch.NewMemoryBatch(
		fn1,
		100,
		time.Second*15,
		2,
	)

	for i := 0; i < 350; i++ {
		err := mBatch.Insert(i)
		if err != nil {
			log.Println("[error]", err)
			break
		}
		time.Sleep(time.Millisecond * 20)
	}

	for {
	}

}
