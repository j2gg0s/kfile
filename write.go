package kfile

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type Line []byte

func NewWriter(topic, name string, client sarama.Client, opts ...Option) (*File, error) {
	if !client.Config().Producer.Return.Successes {
		return nil, fmt.Errorf("kfile requred Producer.Return.Successes is true")
	}

	return newFile(formatURL(topic, name), client, true, opts...)
}

func (f *File) Append(lines ...Line) error {
	for _, line := range lines {
		f.producer.Input() <- &sarama.ProducerMessage{
			Topic: f.Topic,
			Key:   sarama.ByteEncoder(f.Name),
			Value: sarama.ByteEncoder(line),
		}
	}
	f.waitAck += len(lines)

	return nil
}

func (f *File) Close(maxwait time.Duration) (string, error) {
	if len(f.errs) > 0 {
		return "", fmt.Errorf("write with error: %v", f.errs)
	}

	var ticker *time.Ticker
	var start time.Time
	for {
		if f.waitAck == 0 {
			break
		}
		if ticker == nil {
			ticker = time.NewTicker(100 * time.Millisecond)
			if maxwait > 0 {
				start = time.Now()
			}
		}
		<-ticker.C
		if maxwait > 0 && time.Since(start) > maxwait {
			break
		}
	}

	if f.waitAck != 0 {
		return "", fmt.Errorf("message need ack: %d", f.waitAck)
	}

	f.closeFunc()
	if f.producer != nil {
		f.producer.Close()
	}
	if f.consumer != nil {
		f.consumer.Close()
	}

	return f.URI(), nil
}

func (f *File) initProducer() error {
	p, err := sarama.NewAsyncProducerFromClient(f.client)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	f.producer = p

	go func() {
		for {
			select {
			case <-f.ctx.Done():
				return
			case msg := <-f.producer.Successes():
				f.partition = msg.Partition
				if f.sOffset == 0 || f.sOffset > msg.Offset {
					f.sOffset = msg.Offset
				}
				if f.eOffset < msg.Offset {
					f.eOffset = msg.Offset
				}
				f.waitAck -= 1
			case err := <-f.producer.Errors():
				f.errs = append(f.errs, err.Err)
				f.waitAck -= 1
			}
		}
	}()

	return nil
}
