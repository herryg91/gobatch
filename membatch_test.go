package gobatch

import (
    "fmt"
    "time"
    "testing"
)

type testBatchProcessor struct {
    batchesProcssed   int
    elementsProcessed int
}

func (b *testBatchProcessor) process(workerID int, datas []interface{}) {
    fmt.Printf("processing: %v\n", datas)
    b.batchesProcssed   += 1
    b.elementsProcessed += len(datas)
    fmt.Printf("processed: %v\n\n", datas)
}

func TestMemoryBatchMaxSize(t *testing.T) {
    maxSize := 5
    maxWaitSeconds := 10
    workers := 1
    processor := &testBatchProcessor{}
    mbatch := NewMemoryBatch(
		maxSize,
		time.Second*time.Duration(maxWaitSeconds),
		processor.process,
		workers,
	)

    for i:=1; i<=3; i++ {
        mbatch.Insert(fmt.Sprintf("element_%d", i))
    }
    if processor.batchesProcssed != 0 && processor.elementsProcessed != 0 {
        t.Error("expected nothing to be processed but processing happened")
    }
    for i:=4; i<=13; i++ {
        mbatch.Insert(fmt.Sprintf("element_%d", i))
    }
    // TOOD: better way to signal back
    time.Sleep(2 * time.Second)
    if processor.batchesProcssed != 2 && processor.elementsProcessed != 10 {
        t.Errorf("expected 2 batch of total 10 elements, got: %+v", processor)
    }
    for i:=14; i<=15; i++ {
        mbatch.Insert(fmt.Sprintf("element_%d", i))
    }

    // TOOD: better way to signal back
    time.Sleep(2 * time.Second)
    if processor.batchesProcssed != 3 && processor.elementsProcessed != 15 {
        t.Errorf("expected 3 batch of total 15 elements, got: %+v", processor)
    }
}

func TestMemoryBatchMaxWait(t *testing.T) {
    maxSize := 5
    maxWaitSeconds := 10
    workers := 1
    processor := &testBatchProcessor{}
    mbatch := NewMemoryBatch(
		maxSize,
		time.Second*time.Duration(maxWaitSeconds),
		processor.process,
		workers,
	)

    for i:=1; i<=3; i++ {
        mbatch.Insert(fmt.Sprintf("element_%d", i))
    }
    time.Sleep(10 * time.Second)
    if processor.batchesProcssed != 1 && processor.elementsProcessed != 3 {
        t.Errorf("expected 1 batch of total 3 elements, got: %+v", processor)
    }
}
