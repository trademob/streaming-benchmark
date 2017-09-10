package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"bench"
	"bench/requester"
)

func main() {
	var (
		system      = flag.String("s", "", "[kafka, kinesis]")
		rate        = flag.Uint64("r", 1400, "requests per second")
		size        = flag.Int("sz", 200, "message size")
		duration    = flag.Duration("d", 30*time.Second, "benchmark runtime")
		connections = flag.Uint64("c", 1, "connections")
		url         = flag.String("url", "", "broker url")
		burst       = flag.Uint64("b", 1000, "burst for requests")
	)
	flag.Parse()

	var factory bench.RequesterFactory

	switch *system {
	case "kinesis":
		factory = &requester.KinesisStreamingRequesterFactory{
			PayloadSize: *size,
			Stream:      "benchmark",
			Region:      "eu-west-1",
		}
	case "kafka":
		urlSplit := strings.Split(*url, ",")
		factory = &requester.KafkaRequesterFactory{
			URLs:        urlSplit,
			PayloadSize: *size,
			Topic:       "benchmark",
		}
	default:
		fmt.Printf("Unknown system '%s'\n", *system)
		os.Exit(1)
	}
	run(factory, *rate, *connections, *duration, fmt.Sprintf("%s_%d_%d.txt", *system, *rate, *size), *burst)
}

func run(factory bench.RequesterFactory, rate, conns uint64, duration time.Duration,
	output string, burst uint64) {

	benchmark := bench.NewBenchmark(factory, rate, conns, duration, burst)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}
	if err := summary.GenerateLatencyDistribution(nil, output); err != nil {
		panic(err)
	}
	fmt.Println(summary)
}
