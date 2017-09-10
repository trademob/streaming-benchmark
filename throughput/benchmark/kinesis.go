package benchmark

import (
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisBenchmark struct {
	payloadSize       uint
	stream            *string
	region            *string
	recv              chan []byte
	partitionKey      *string
	conn              *kinesis.Kinesis
	msg               *kinesis.PutRecordInput
	shardInp          *kinesis.GetShardIteratorInput
	shardItNext       *string
	errors            uint
	acked             uint
	numMsgs           uint
	sendDone          chan bool
	ShardId           *string
	ShardIteratorType *string
}

func NewKinesisBenchmark(region string, stream string, payloadSize uint) *KinesisBenchmark {
	return &KinesisBenchmark{
		payloadSize:       payloadSize,
		region:            aws.String(region),
		partitionKey:      aws.String("key1"),
		stream:            aws.String(stream),
		recv:              make(chan []byte, 65536),
		sendDone:          make(chan bool),
		ShardId:           aws.String("0"),
		ShardIteratorType: aws.String("LATEST"),
	}
}

func (k *KinesisBenchmark) Setup(consumer bool, numMsgs uint) error {
	k.numMsgs = numMsgs
	if consumer {
		return k.setupConsumer()
	}
	return k.setupProducer()

}

func (k *KinesisBenchmark) setupProducer() error {
	s := session.New(&aws.Config{Region: k.region})
	kc := kinesis.New(s)

	k.conn = kc

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

func (k *KinesisBenchmark) Send() error {

	go func() {
		putOutput, err := k.conn.PutRecord(k.msg)
		if err != nil {
			fmt.Println(err)
			k.errors++
		}
		k.acked++
		if k.acked == k.numMsgs {
			fmt.Println("ShardId: ", *putOutput.ShardId)
			k.sendDone <- true
		}
	}()
	return nil
}

func (k *KinesisBenchmark) setupConsumer() error {
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

	go func() {
		fmt.Println("Begin consumer request...")
		for k.shardItNext != nil {
			k.getRecs()
		}
	}()

	return nil
}

func (k *KinesisBenchmark) getRecs() error {

	records, err := k.conn.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: k.shardItNext,
	})

	if err != nil {
		fmt.Println(err)
		k.errors++
	}

	for _, msg := range records.Records {
		k.recv <- msg.Data
	}

	k.shardItNext = records.NextShardIterator
	return nil
}

func (k *KinesisBenchmark) Recv() <-chan []byte {
	return k.recv
}

func (k *KinesisBenchmark) Errors() uint {
	return k.errors
}

func (k *KinesisBenchmark) SendDone() <-chan bool {
	return k.sendDone
}
