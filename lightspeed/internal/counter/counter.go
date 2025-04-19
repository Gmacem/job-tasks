package counter

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type IpsCounter interface {
	CountIps(filename string) (uint64, error)
}

type SequentialCounter struct {
	bm *bitmap
}

func NewSequential() *SequentialCounter {
	return &SequentialCounter{
		bm: newBitmap(),
	}
}

func (c *SequentialCounter) CountIps(filename string) (uint64, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		ip := strings.TrimSpace(scanner.Text())
		ipInt, err := ipToUint32(ip)
		if err != nil {
			log.Printf("Warning: Invalid IP address '%s': %v", ip, err)
			continue
		}

		c.bm.set(ipInt)
	}

	return c.bm.countSetBits(), scanner.Err()
}
