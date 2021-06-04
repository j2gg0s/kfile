package kfile

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	defaultNumPartitions     = 6
	defaultReplicationFactor = 1
)

type File struct {
	Name        []byte
	Topic       string
	topicDetail *sarama.TopicDetail

	ctx       context.Context
	closeFunc context.CancelFunc

	client   sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.PartitionConsumer

	// number of messages wait to ack
	waitAck int
	errs    []error

	partition int32
	sOffset   int64
	eOffset   int64
}

type Option func(*File)

func NewWriter(topic, name string, client sarama.Client, opts ...Option) (*File, error) {
	return newFile(formatURL(topic, name), client, true, opts...)
}

func NewReader(addr string, client sarama.Client, opts ...Option) (*File, error) {
	return newFile(addr, client, false, opts...)
}

func newFile(addr string, client sarama.Client, isW bool, opts ...Option) (*File, error) {
	f, err := parseURL(addr)
	if err != nil {
		return nil, err
	}

	f.client = client
	f.ctx, f.closeFunc = context.WithCancel(context.Background())
	f.topicDetail = &sarama.TopicDetail{
		NumPartitions:     defaultNumPartitions,
		ReplicationFactor: defaultReplicationFactor,
	}

	for _, opt := range opts {
		opt(f)
	}

	if isW {
		if err := f.initProducer(); err != nil {
			return nil, err
		}
	} else {
		if err := f.initConsumer(); err != nil {
			return nil, err
		}
	}

	if err := f.createTopicIfNotExist(); err != nil {
		return nil, fmt.Errorf("create topic: %w", err)
	}

	return f, nil
}
