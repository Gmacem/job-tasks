package counter

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
)

type ParallelCounter struct {
	bm *bitmap
}

func NewParallel() *ParallelCounter {
	return &ParallelCounter{
		bm: newBitmap(),
	}
}

const (
	batchSize     = 10000
	windowSize    = 1 << 30
	maxWindows    = 4
	windowBufSize = 1000
)

func (c *ParallelCounter) CountIps(filename string) (uint64, error) {
	file, err := os.Stat(filename)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %v", err)
	}
	fileSize := file.Size()

	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()

	numWindows := (fileSize + windowSize - 1) / windowSize

	results := make(chan []uint32, windowBufSize)
	errorsCh := make(chan error, maxWindows)
	windowCh := make(chan int64, maxWindows)

	var wg sync.WaitGroup

	for i := 0; i < maxWindows; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for windowNum := range windowCh {
				windowStart := windowNum * windowSize
				// capture more context if seek hits the middle of an ip address
				windowEnd := windowStart + windowSize + 16
				if windowEnd > fileSize {
					windowEnd = fileSize
				}
				currentWindowSize := windowEnd - windowStart
				data, err := syscall.Mmap(int(f.Fd()), windowStart, int(currentWindowSize), syscall.PROT_READ, syscall.MAP_SHARED)
				if err != nil {
					errorsCh <- fmt.Errorf("error mapping window %d: %v", windowNum, err)
					return
				}

				err = c.processWindow(data, results, windowStart != 0, windowEnd != fileSize)
				syscall.Munmap(data)
				if err != nil {
					errorsCh <- fmt.Errorf("error processing window %d: %v", windowNum, err)
					return
				}
			}
		}()
	}

	go func() {
		for i := int64(0); i < numWindows; i++ {
			windowCh <- i
		}
		close(windowCh)
	}()

	go func() {
		wg.Wait()
		close(results)
		close(errorsCh)
	}()

	for {
		select {
		case batch, ok := <-results:
			if !ok {
				return c.bm.countSetBits(), nil
			}
			for _, ip := range batch {
				c.bm.set(ip)
			}
		case err := <-errorsCh:
			if err != nil {
				return 0, fmt.Errorf("window processing error: %v", err)
			}
		}
	}
}

// skip_first and skip_last are used to skip the first and last ip which were only partially included
func (c *ParallelCounter) processWindow(data []byte, results chan []uint32, skip_first bool, skip_last bool) error {
	batch := make([]uint32, 0, batchSize)

	start := 0
	for {
		end := bytes.IndexByte(data[start:], '\n')
		if skip_first {
			skip_first = false
			start = end + 1
			continue
		}
		if end == -1 {
			if start < len(data) && !skip_last {
				line := strings.TrimSpace(string(data[start:]))
				if parsedIp, err := ipToUint32(line); err == nil {
					batch = append(batch, parsedIp)
				}
			}
			break
		}

		line := strings.TrimSpace(string(data[start : start+end]))
		if parsedIp, err := ipToUint32(line); err == nil {
			batch = append(batch, parsedIp)
		}

		if len(batch) >= batchSize {
			results <- batch
			batch = make([]uint32, 0, batchSize)
		}

		start += end + 1
		if start >= len(data) {
			break
		}
	}

	if len(batch) > 0 {
		results <- batch
	}

	return nil
}
