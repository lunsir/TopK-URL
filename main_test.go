package main

import (
	"container/heap"
	"os"
	"sync"
	"testing"
)

func TestPq(t *testing.T) {
	pq := make(PriorityQueue, 0)

	heap.Init(&pq)
	heap.Push(&pq, &URLCnt{"www.baidu.com", 12})
	heap.Push(&pq, &URLCnt{"www.baid.com", 2})
	len := pq.Len()
	if len == 2 {
		t.Log("Pass")
	} else {
		t.Errorf("pq lenth get %d want 2", len)
	}

	heap.Push(&pq, &URLCnt{"www.badu.com", 11})
	urlCnt := heap.Pop(&pq).(*URLCnt)
	if urlCnt.url == "www.baid.com" && urlCnt.cnt == 2 {
		t.Log("Pass")
	} else {
		t.Errorf("url want www.baid.com  get %s, count want 2 get %d", urlCnt.url, urlCnt.cnt)
	}
}

func TestMergePq(t *testing.T) {
	pq1 := make(PriorityQueue, 0)
	heap.Init(&pq1)
	heap.Push(&pq1, &URLCnt{"www.baidu.com", 12})
	heap.Push(&pq1, &URLCnt{"www.baid.com", 2})

	pq2 := make(PriorityQueue, 0)
	heap.Init(&pq2)
	heap.Push(&pq2, &URLCnt{"www.alibaba.com", 8})
	heap.Push(&pq2, &URLCnt{"www.alijiujiu.com", 9})

	pq3 := mergeQueue(pq1, pq2, 3)
	len3 := pq3.Len()
	if len3 == 3 {
		t.Log("Pass")
	} else {
		t.Errorf("pq lenth get %d want 3", len3)
	}
	urlCnt := heap.Pop(&pq3).(*URLCnt)
	if urlCnt.url == "www.alibaba.com" && urlCnt.cnt == 8 {
		t.Log("Pass")
	} else {
		t.Errorf("url want www.alibaba.com get %s, count want 8 get %d", urlCnt.url, urlCnt.cnt)
	}
}

func TestReadFileProducera(t *testing.T) {
	var errChan = make(chan error, 2)
	var bufChan = make(chan string, 16*1024)
	go readFileProducer("./input.txt", bufChan, errChan)
	line1 := <-bufChan
	for i := 1; i < 1473; i++ {
		<-bufChan
	}
	line1474 := <-bufChan

	if line1 == "aaa" && line1474 == "ccc" {
		t.Log("Pass")
	} else {
		t.Errorf("line1 get %s want aaa, line1474 get %s want ccc", line1, line1474)
	}
}

func TestWriteFileConsumer(t *testing.T) {
	var waitgroup sync.WaitGroup
	var errChan = make(chan error, 2)
	var bufChan = make(chan string, 16*1024)
	waitgroup.Add(1)
	go func() {
		writeFileConsumer("/tmp/TestWriteFileConsumer.txt", bufChan, 1, errChan)
		waitgroup.Done()
	}()

	//write more than 4k
	for i := 0; i < 205; i++ {
		bufChan <- "123456789"
		bufChan <- "abcdefghi"
	}
	close(bufChan)
	waitgroup.Wait()

	fi, err := os.Stat("/tmp/TestWriteFileConsumer.txt-0")
	if err != nil {
		t.Errorf(err.Error())
	}
	fileSize := fi.Size()
	if fileSize == 4100 {
		t.Log("Pass")
	} else {
		t.Errorf("file size want 4100 get %d", fileSize)
	}
}

func TestHshAggFileLines(t *testing.T) {
	pq, errChan := hashAggFileLines("./input.txt", true, 6)
	close(errChan)
	if len(errChan) != 0 {
		t.Errorf("hash Agg input.text failed")
	}
	len := pq.Len()
	if len == 6 {
		t.Log("Pass")
	} else {
		t.Errorf("pq lenth get %d want 6", len)
	}
	urlCnt := heap.Pop(&pq).(*URLCnt)
	if urlCnt.url == "ccc" && urlCnt.cnt == 23 {
		t.Log("Pass")
	} else {
		t.Errorf("url want ccc get %s, count want 23 get %d", urlCnt.url, urlCnt.cnt)
	}
}
