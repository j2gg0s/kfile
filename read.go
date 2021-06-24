package kfile

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

func NewReader(addr string, client sarama.Client, opts ...Option) (*File, error) {
	return newFile(addr, client, false, opts...)
}

func (f *File) ReadLine() (chan Line, error) {
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
			if msg.Offset == f.eOffset {
				break
			}
		}
		close(ch)
	}()

	return ch, nil
}

func (f *File) ReadAll(maxwait time.Duration) (*bytes.Buffer, error) {
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
		if msg.Offset == f.eOffset {
			return buf, nil
		}
	}

	return buf, nil
}

func (f *File) initConsumer() error {
	consumer, err := sarama.NewConsumerFromClient(f.client)
	if err != nil {
		return fmt.Errorf("new consumer: %w", err)
	}
	pconsumer, err := consumer.ConsumePartition(f.Topic, f.partition, f.sOffset)
	if err != nil {
		if kerr, ok := err.(sarama.KError); ok && kerr == sarama.ErrOffsetOutOfRange {
			return fmt.Errorf("%w: %s", ErrNotFound, err.Error())
		}
		return fmt.Errorf("consume partiton: %w", err)
	}
	f.consumer = pconsumer
	return nil
}
