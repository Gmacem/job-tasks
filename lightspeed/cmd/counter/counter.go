package main

import (
	"flag"
	"log"

	"lightspeed/internal/counter"
)

func main() {
	mode := flag.String("mode", "seq", "processing mode: seq, parallel")
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		log.Fatal("Usage: counter [-mode seq|parallel|fast|turbo|extreme] <input_file>")
	}

	var ipsCounter counter.IpsCounter
	switch *mode {
	case "parallel":
		ipsCounter = counter.NewParallel()
	case "seq":
		ipsCounter = counter.NewSequential()
	default:
		log.Fatalf("Unknown mode: %s", *mode)
		return
	}

	unique, err := ipsCounter.CountIps(args[0])
	if err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	log.Printf("Found %d unique IP addresses", unique)
}
