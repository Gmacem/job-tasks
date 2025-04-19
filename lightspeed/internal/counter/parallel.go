package counter

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

type ParallelCounter struct {
	bm *bitmap
}

func NewParallel() *ParallelCounter {
	return &ParallelCounter{
		bm: newBitmap(),
	}
}

type chunk struct {
	Start int64
	Size  int64
}

func (c *ParallelCounter) CountIps(filename string) (uint64, error) {
	file, err := os.Stat(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	fileSize := file.Size()
	numWorkers := runtime.NumCPU()
	chunkSize := (fileSize + int64(numWorkers) - 1) / int64(numWorkers)
	if chunkSize == 0 {
		chunkSize = fileSize
		numWorkers = 1
	}

	results := make(chan uint32, 10000)
	errorsCh := make(chan error, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		startChunk := int64(i) * int64(chunkSize)

		go func(start int64) {
			defer wg.Done()
			err = c.processChunk(chunk{start, chunkSize}, filename, results)
			if err != nil {
				errorsCh <- err
			}
		}(startChunk)
	}

	go func() {
		wg.Wait()
		close(results)
		close(errorsCh)
	}()

	for {
		select {
		case res, ok := <-results:
			if !ok {
				return c.bm.countSetBits(), nil
			}
			c.bm.set(res)
		case err := <-errorsCh:
			if err != nil {
				return 0, fmt.Errorf("worker error: %v", err)
			}
		}
	}
}

func (c *ParallelCounter) processChunk(chk chunk, filename string, results chan uint32) error {
	fmt.Println("Processing chunk", chk.Start, chk.Size)
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	_, err = file.Seek(chk.Start, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking file: %v", err)
	}
	limitedReader := io.LimitReader(bufio.NewReader(file), chk.Size+int64(16)) // fix seek in middle of ip
	scanner := bufio.NewScanner(limitedReader)

	for scanner.Scan() {
		rawIp := strings.TrimSpace(scanner.Text())
		parsedIp, err := ipToUint32(rawIp)
		if err != nil {
			log.Printf("Perhaps seek got inside the IP address, invalid IP address '%s': %v", rawIp, err)
			continue
		}
		results <- parsedIp
	}
	return nil
}
