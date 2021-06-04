package kfile

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func (f *File) createTopicIfNotExist() error {
	// NOTE: don't close admin, it will close client
	admin, err := sarama.NewClusterAdminFromClient(f.client)
	if err != nil {
		return fmt.Errorf("create cluster admin: %w", err)
	}
	return createTopicIfNotExist(f.Topic, f.topicDetail, admin)
}

func createTopicIfNotExist(topic string, topicDetail *sarama.TopicDetail, admin sarama.ClusterAdmin) error {
	metas, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		return fmt.Errorf("describe topics: %w", err)
	}

	if metas[0].Err == sarama.ErrUnknownTopicOrPartition {
		if err = admin.CreateTopic(topic, topicDetail, false); err != nil {
			return fmt.Errorf("create topic: %w", err)
		}
	}

	return nil
}

// WithNumpartitions
// Only valid if topic not exist and create by kfile.
func WithNumPartitions(np int32) Option {
	return func(f *File) {
		f.topicDetail.NumPartitions = np
	}
}

// WithReplicationFactor
// Only valid if topic not exist and create by kfile.
func WithReplicationFactor(rf int16) Option {
	return func(f *File) {
		f.topicDetail.ReplicationFactor = rf
	}
}

// WithTopicRetention
// Only valid if topic not exist and create by kfile.
func WithTopicRetention(d time.Duration) Option {
	return func(f *File) {
		if len(f.topicDetail.ConfigEntries) == 0 {
			f.topicDetail.ConfigEntries = map[string]*string{}
		}
		retentionMs := strconv.Itoa(int(d / time.Millisecond))
		f.topicDetail.ConfigEntries["retention.ms"] = &retentionMs
	}
}
