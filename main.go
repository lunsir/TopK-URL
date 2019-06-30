package main

import (
	"./utils"
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sync"
)

var memSize int64 = 1 * 1024 * 1024 * 1024
var urlAvgSize = 64

//URLCnt is the type we manage in a priority queue.
type URLCnt struct {
	url string
	cnt int64
}

//PriorityQueue implements heap. Interface and holds Items.
type PriorityQueue []*URLCnt

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not highest, count so we use less than here.
	return pq[i].cnt < pq[j].cnt
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

//Push a URLCnt item
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*URLCnt)
	*pq = append(*pq, item)
}

//Pop lowest count URLCnt item
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func readFileProducer(filePath string, bufChan chan string, errChan chan error) {
	defer close(bufChan)

	f, err := os.Open(filePath)
	//fmt.Println("open file:", filePath)
	defer f.Close()
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(filePath + " not exits, please check if someone has removed it or caused by data skew")
			return
		}
		errChan <- err
		return
	}
	fileScanner := bufio.NewScanner(f)
	//count := 0
	buf := make([]byte, 256*1024)
	fileScanner.Buffer(buf, 256*1024)
	for fileScanner.Scan() {
		bufChan <- fileScanner.Text()
		//count++
	}
	//fmt.Println("read line:", count)
}

func writeFileConsumer(filePath string, bufChan chan string, shardCnt uint32, errChan chan error) {
	openFileMap := make(map[uint32]*bufio.Writer)
	//count := 0

	for line := range bufChan {
		fileNum := utils.StringHash(line) % shardCnt
		w, ok := openFileMap[fileNum]
		if !ok {
			f, err := os.OpenFile(filePath+"-"+fmt.Sprint(fileNum), os.O_CREATE|os.O_WRONLY, 0666)
			defer f.Close()
			//fmt.Println("create file:", filePath+"-"+fmt.Sprint(fileNum))
			if err != nil {
				errChan <- err
				return
			}
			w = bufio.NewWriterSize(bufio.NewWriter(f), 256*1024)
			defer w.Flush()
			openFileMap[fileNum] = w
		}
		fmt.Fprintln(w, line)
		//count++
	}
	//fmt.Println("write line:", count)
}

func shardingFileByHash(filePath string) (uint32, chan error) {
	var waitgroup sync.WaitGroup
	var errChan = make(chan error, 2)

	fi, err := os.Stat(filePath)
	if err != nil {
		errChan <- err
		return 0, errChan
	}
	fileSize := fi.Size()
	shardCnt := uint32((fileSize + memSize - 1) / memSize)
	//fmt.Println("shard count :", shardCnt)
	if shardCnt == 1 {
		return shardCnt, errChan
	}

	var bufChan = make(chan string, memSize/int64(urlAvgSize))
	//Producer
	go readFileProducer(filePath, bufChan, errChan)
	//Consumer
	waitgroup.Add(1)
	go func() {
		writeFileConsumer(filePath, bufChan, shardCnt, errChan)
		waitgroup.Done()
	}()
	waitgroup.Wait()

	return shardCnt, errChan
}

func getPqFromMap(cntMap map[string]int64, queueLen int) PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	for url, cnt := range cntMap {
		heap.Push(&pq, &URLCnt{url: url, cnt: cnt})
		if pq.Len() > queueLen {
			heap.Pop(&pq)
		}
	}
	return pq
}

func hashAggConsumer(lines chan string, wg *sync.WaitGroup, cntMap map[string]int64) {
	defer wg.Done()
	for line := range lines {
		if _, ok := cntMap[line]; ok {
			cntMap[line]++
		} else {
			cntMap[line] = 1
		}
	}
}

func mergeQueue(firstQueue PriorityQueue, secondQueue PriorityQueue, limit int) PriorityQueue {
	var secondQueueMax *URLCnt
	for secondQueue.Len() > 0 && firstQueue.Len() < limit {
		secondQueueMax = heap.Remove(&secondQueue, secondQueue.Len()-1).(*URLCnt)
		heap.Push(&firstQueue, secondQueueMax)
	}
	if secondQueue.Len() == 0 {
		return firstQueue
	}

	firstQueueMin := heap.Pop(&firstQueue).(*URLCnt)
	for secondQueue.Len() > 0 {
		secondQueueMax = heap.Remove(&secondQueue, secondQueue.Len()-1).(*URLCnt)
		if secondQueueMax.cnt < firstQueueMin.cnt {
			break
		}
		heap.Push(&firstQueue, secondQueueMax)
		firstQueueMin = heap.Pop(&firstQueue).(*URLCnt)
	}
	heap.Push(&firstQueue, firstQueueMin)
	return firstQueue
}

func hashAggFileLines(filePath string, canSplitFile bool, limit int) (PriorityQueue, chan error) {
	var shardCnt uint32
	var errChan chan error
	if canSplitFile {
		shardCnt, errChan = shardingFileByHash(filePath)
		if len(errChan) != 0 {
			return nil, errChan
		}
	}

	if !canSplitFile || shardCnt == 1 {
		var bufChan = make(chan string, memSize/int64(urlAvgSize))
		var errChan = make(chan error, 2)

		go readFileProducer(filePath, bufChan, errChan)
		var waitgroup sync.WaitGroup
		waitgroup.Add(1)
		cntMap := make(map[string]int64)
		go hashAggConsumer(bufChan, &waitgroup, cntMap)
		waitgroup.Wait()

		if len(errChan) != 0 {
			return nil, errChan
		}
		return getPqFromMap(cntMap, limit), errChan
	}

	var finalQueue PriorityQueue
	var tmpQueue PriorityQueue
	var i uint32
	for ; i < shardCnt; i++ {
		tmpQueue, errChan = hashAggFileLines(filePath+"-"+fmt.Sprint(i), false, limit)
		if len(errChan) != 0 {
			return nil, errChan
		}
		if finalQueue == nil {
			finalQueue = tmpQueue
		} else {
			finalQueue = mergeQueue(finalQueue, tmpQueue, limit)
		}
	}

	return finalQueue, errChan
}

func main() {
	pq, errChan := hashAggFileLines("./input.txt", true, 100)
	close(errChan)
	if len(errChan) != 0 {
		for err := range errChan {
			fmt.Println(err.Error())
		}
		return
	}
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*URLCnt)
		fmt.Printf("url: %s----count:%d\n", item.url, item.cnt)
	}
}
