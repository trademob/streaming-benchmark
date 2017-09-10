package requester

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"bench"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// KinesisStreamingRequesterFactory implements RequesterFactory by creating a
// Requester which publishes messages to Kinesis and waits to receive
// them.
type KinesisStreamingRequesterFactory struct {
	PayloadSize int
	Stream      string
	Region      string
}

// GetRequester returns a new Requester, called for each Benchmark connection.
func (k *KinesisStreamingRequesterFactory) GetRequester(num uint64) bench.Requester {
	return &kinesisStreamingRequester{
		stream:            aws.String(k.Stream + "-" + strconv.FormatUint(num, 10)),
		region:            aws.String(k.Region),
		partitionKey:      aws.String("key1"),
		payloadSize:       uint(k.PayloadSize),
		subDone:           make(chan bool),
		ShardId:           aws.String("0"),
		ShardIteratorType: aws.String("LATEST"),
	}
}

// KinesisStreamingRequester implements Requester by publishing a message to NATS
// Streaming and waiting to receive it.
type kinesisStreamingRequester struct {
	stream            *string
	region            *string
	partitionKey      *string
	payloadSize       uint
	conn              *kinesis.Kinesis
	msg               *kinesis.PutRecordInput
	msgChan           chan []byte
	subDone           chan bool
	ShardId           *string
	ShardIteratorType *string
	shardInp          *kinesis.GetShardIteratorInput
	shardItNext       *string
}

// Setup prepares the Requester for benchmarking.
func (k *kinesisStreamingRequester) Setup() error {
	s := session.New(&aws.Config{Region: k.region})
	kc := kinesis.New(s)

	k.conn = kc

	k.shardInp = &kinesis.GetShardIteratorInput{
		ShardId:           k.ShardId,
		ShardIteratorType: k.ShardIteratorType,
		StreamName:        k.stream,
	}

	iteratorOutput, err := kc.GetShardIterator(k.shardInp)

	if err != nil {
		fmt.Println(err)
		return err
	}

	k.shardItNext = iteratorOutput.ShardIterator

	k.msgChan = make(chan []byte)
	go func() {
		fmt.Println("Begin consumer request...")
		for k.shardItNext != nil {
			select {
			case <-k.subDone:
				return
			default:
				k.getRecs()
			}
		}
	}()

	msg := make([]byte, k.payloadSize)
	for i := 0; i < int(k.payloadSize); i++ {
		msg[i] = 'A' + uint8(rand.Intn(26))
	}
	k.msg = &kinesis.PutRecordInput{
		Data:         []byte(msg),
		StreamName:   k.stream,
		PartitionKey: k.partitionKey,
	}
	return nil
}

// Request performs a synchronous request to the system under test.
func (k *kinesisStreamingRequester) Request() error {
	k.send()
	select {
	case <-k.msgChan:
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("timeout")
	}
}

func (k *kinesisStreamingRequester) getRecs() error {

	records, err := k.conn.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: k.shardItNext,
	})

	if err != nil {
		fmt.Println(err)
	}

	for _, msg := range records.Records {
		k.msgChan <- msg.Data
	}

	k.shardItNext = records.NextShardIterator
	// time.Sleep(time.Millisecond * 200)
	return nil
}

func (k *kinesisStreamingRequester) send() error {

	_, err := k.conn.PutRecord(k.msg)

	if err != nil {
		fmt.Println(err)
	}

	return nil
}

// Teardown is called upon benchmark completion.
func (k *kinesisStreamingRequester) Teardown() error {
	k.subDone <- true
	k.conn = nil
	return nil
}
