package counter

import (
	"bytes"
	"fmt"
	"log"
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
	windowSize    = 1 << 30       // 1GB window size
	maxWindows    = 4             // Maximum concurrent windows (adjust based on available RAM)
	windowBufSize = batchSize * 2 // Buffer size for window results channel
)

func (c *ParallelCounter) CountIps(filename string) (uint64, error) {
	fmt.Println("Start parallel processing")
	file, err := os.Stat(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	fileSize := file.Size()

	// Open file for memory mapping
	f, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return 0, fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()

	// Calculate number of windows needed
	numWindows := (fileSize + windowSize - 1) / windowSize

	// Create channels for window processing
	results := make(chan []uint32, windowBufSize)
	errorsCh := make(chan error, maxWindows)
	windowCh := make(chan int64, maxWindows)

	var wg sync.WaitGroup

	// Start window processor goroutines
	for i := 0; i < maxWindows; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for windowNum := range windowCh {
				windowStart := windowNum * windowSize
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

				err = c.processWindow(data, results)
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

func (c *ParallelCounter) processWindow(data []byte, results chan []uint32) error {
	batch := make([]uint32, 0, batchSize)

	start := 0
	for {
		end := bytes.IndexByte(data[start:], '\n')
		if end == -1 {
			if start < len(data) {
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
			batch = batch[:0]
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
