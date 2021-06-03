package kfile

import (
	"bytes"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Shopify/sarama"
)

type File struct {
	Name   []byte
	Topic  string
	client sarama.Client

	producer sarama.SyncProducer
	consumer sarama.PartitionConsumer

	partition int32
	sOffset   int64
	eOffset   int64
}

type Line []byte

func (f *File) Write(lines ...Line) error {
	if f.producer == nil {
		if err := f.initProducer(); err != nil {
			return err
		}
	}

	msgs := make([]*sarama.ProducerMessage, len(lines))
	for i, line := range lines {
		msgs[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(f.Name),
			Value: sarama.ByteEncoder(line),
		}
	}
	if err := f.producer.SendMessages(msgs); err != nil {
		return fmt.Errorf("send messages: %w", err)
	}

	for _, msg := range msgs {
		f.partition = msg.Partition
		if f.sOffset == 0 || f.sOffset > msg.Offset {
			f.sOffset = msg.Offset
		}
		if f.eOffset < msg.Offset {
			f.eOffset = msg.Offset
		}
	}

	return nil
}

func (f *File) ReadAsync() (chan Line, error) {
	if f.consumer == nil {
		if err := f.initConsumer(); err != nil {
			return nil, err
		}
	}

	ch := make(chan Line)
	go func() {
		for msg := range f.consumer.Messages() {
			if !bytes.Equal(msg.Key, f.Name) {
				continue
			}
			if msg.Offset > f.eOffset {
				break
			}
			ch <- Line(msg.Value)
		}
		close(ch)
	}()

	return ch, nil
}

func (f *File) ReadSync() (*bytes.Buffer, error) {
	if f.consumer == nil {
		if err := f.initConsumer(); err != nil {
			return nil, err
		}
	}

	buf := bytes.NewBuffer(nil)
	for msg := range f.consumer.Messages() {
		if !bytes.Equal(msg.Key, f.Name) {
			continue
		}
		if msg.Offset > f.eOffset {
			return buf, nil
		}
		if _, err := buf.Write(msg.Value); err != nil {
			return nil, fmt.Errorf("write to buffer: %w", err)
		}
	}

	return buf, nil
}

func (f *File) URI() string {
	return fmt.Sprintf(
		"kfile://%s?name=%s&partition=%d&start=%d&end=%d",
		f.Topic, string(f.Name), f.partition, f.sOffset, f.eOffset)
}

func (f *File) Close() error {
	return nil
}

// for write
func NewFile(name, topic string, client sarama.Client) (*File, error) {
	// TODO: 检查 client 的配置, hash key -> partition
	file := File{
		Name:   []byte(name),
		Topic:  topic,
		client: client,
	}

	return &file, nil
}

// for read
func NewFileFromURI(addr string, client sarama.Client) (*File, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("prase uri: %w", err)
	}
	file := File{
		client: client,
	}
	file.Topic = u.Host

	q := u.Query()
	if v, ok := q["name"]; ok && len(v) > 0 {
		file.Name = []byte(v[0])
	} else {
		return nil, fmt.Errorf("name is reuqired: %s", addr)
	}

	if v, ok := q["partition"]; ok && len(v) > 0 {
		if i, err := strconv.ParseInt(v[0], 10, 32); err != nil {
			return nil, fmt.Errorf("invalid partition %s: %w", v[0], err)
		} else {
			file.partition = int32(i)
		}
	} else {
		return nil, fmt.Errorf("partition is required: %s", addr)
	}

	getInt64 := func(k string) (int64, error) {
		if v, ok := q[k]; ok && len(v) > 0 {
			if i, err := strconv.ParseInt(v[0], 10, 64); err != nil {
				return 0, fmt.Errorf("invalid %s %s: %w", k, v[0], err)
			} else {
				return i, nil
			}
		} else {
			return 0, fmt.Errorf("%s is required: %s", k, addr)
		}
	}
	if sOffset, err := getInt64("start"); err != nil {
		return nil, err
	} else {
		file.sOffset = sOffset
	}
	if eOffset, err := getInt64("end"); err != nil {
		return nil, err
	} else {
		file.eOffset = eOffset
	}

	return &file, nil
}

func (f *File) initProducer() error {
	p, err := sarama.NewSyncProducerFromClient(f.client)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	f.producer = p
	return nil
}

func (f *File) initConsumer() error {
	consumer, err := sarama.NewConsumerFromClient(f.client)
	if err != nil {
		return fmt.Errorf("new consumer: %w", err)
	}
	pconsumer, err := consumer.ConsumePartition(f.Topic, f.partition, f.sOffset)
	if err != nil {
		return fmt.Errorf("consume partiton: %w", err)
	}
	f.consumer = pconsumer
	return nil
}
