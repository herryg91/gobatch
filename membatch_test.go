package gobatch

import (
	"fmt"
	"testing"
	"time"
)

type testBatchProcessor struct {
	batchesProcessed  int
	elementsProcessed int
}

func (b *testBatchProcessor) process(workerID int, datas []interface{}) {
	fmt.Printf("processing: %v\n", datas)
	b.batchesProcessed += 1
	b.elementsProcessed += len(datas)
	fmt.Printf("processed: %v\n\n", datas)
}

// TOOD: better way to signal back
func waitForFlush(t int) {
	time.Sleep(time.Duration(t) * time.Second)
}

func TestMaxSize(t *testing.T) {
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

	for i := 1; i <= 3; i++ {
		mbatch.Insert(fmt.Sprintf("element_%d", i))
	}
	waitForFlush(2)
	if processor.batchesProcessed != 0 && processor.elementsProcessed != 0 {
		t.Error("expected nothing to be processed but processing happened")
	}
	for i := 4; i <= 13; i++ {
		mbatch.Insert(fmt.Sprintf("element_%d", i))
	}

	waitForFlush(2)
	if processor.batchesProcessed != 2 && processor.elementsProcessed != 10 {
		t.Errorf("expected 2 batch of total 10 elements, got: %+v", processor)
	}
	for i := 14; i <= 15; i++ {
		mbatch.Insert(fmt.Sprintf("element_%d", i))
	}

	waitForFlush(2)
	if processor.batchesProcessed != 3 && processor.elementsProcessed != 15 {
		t.Errorf("expected 3 batch of total 15 elements, got: %+v", processor)
	}
}

func TestMaxWait(t *testing.T) {
	maxSize := 5
	maxWaitSeconds := 7
	workers := 1
	processor := &testBatchProcessor{}
	mbatch := NewMemoryBatch(
		maxSize,
		time.Second*time.Duration(maxWaitSeconds),
		processor.process,
		workers,
	)

	for i := 1; i <= 3; i++ {
		mbatch.Insert(fmt.Sprintf("element_%d", i))
	}

	waitForFlush(9)
	if processor.batchesProcessed != 1 && processor.elementsProcessed != 3 {
		if processor.batchesProcessed != 1 {
			fmt.Println(processor.batchesProcessed)
		}
		t.Errorf("expected 1 batch of total 3 elements, got: %+v", processor)
	}
}

func TestFlushInsert(t *testing.T) {
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

	for i := 1; i <= 3; i++ {
		mbatch.Insert(fmt.Sprintf("element_%d", i))
	}
	mbatch.FlushInsert("element_4")

	waitForFlush(2)
	if processor.batchesProcessed != 1 && processor.elementsProcessed != 3 {
		t.Errorf("expected 1 batch of total 3 elements, got: %+v", processor)
	}
}
