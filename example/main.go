package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/j2gg0s/kfile"
	"github.com/spf13/cobra"
)

var (
	client            sarama.Client
	addrs             []string
	topic             string
	concurrent        int
	numPartitions     int
	replicationFactor int
)

func main() {
	cmd := cobra.Command{
		Use: "kfile",
	}

	cmd.PersistentFlags().StringSliceVar(&addrs, "addr", []string{}, "kafka addrs")
	cmd.PersistentFlags().StringVar(&topic, "topic", "kfile.default", "kafka topic")
	cmd.PersistentFlags().IntVar(&concurrent, "concurrent", 3, "write/read concurrent number")
	cmd.PersistentFlags().IntVar(&numPartitions, "numPartitions", 6, "number of partitions")
	cmd.PersistentFlags().IntVar(&replicationFactor, "replicationFactor", 1, "recplication factor")

	cmd.PersistentPreRunE = func(*cobra.Command, []string) error {
		cfg := sarama.NewConfig()
		cfg.Producer.Return.Successes = true

		var err error
		client, err = sarama.NewClient(addrs, cfg)
		if err != nil {
			return fmt.Errorf("new sarama client: %w", err)
		}
		return nil
	}

	cmd.AddCommand(write(), read())

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

func write() *cobra.Command {
	cmd := cobra.Command{Use: "write"}

	cmd.RunE = func(*cobra.Command, []string) error {
		wg := sync.WaitGroup{}
		for i := 0; i < concurrent; i++ {
			wg.Add(1)
			go func(ind int) {
				defer wg.Done()
				name := fmt.Sprintf("file%d%d", time.Now().Day(), ind)
				writer, err := kfile.NewWriter(
					topic, name,
					client,
					kfile.WithNumPartitions(int32(numPartitions)),
					kfile.WithReplicationFactor(int16(replicationFactor)),
					kfile.WithTopicRetention(time.Hour*24*31))
				if err != nil {
					fmt.Println(fmt.Errorf("new file: %w", err))
					return
				}
				for j := 0; j < 100; j++ {
					err := writer.Append(
						kfile.Line([]byte(fmt.Sprintf("Line %d of file %s\n", j, name))))
					if err != nil {
						fmt.Println(fmt.Errorf("write: %w", err))
						return
					}
				}
				uri, err := writer.Close(10 * time.Second)
				if err != nil {
					fmt.Println(fmt.Errorf("close file: %w", err))
					return
				}

				fmt.Printf("write to %s\n", uri)
			}(i)
		}
		wg.Wait()
		return nil
	}

	return &cmd
}

func read() *cobra.Command {
	cmd := cobra.Command{Use: "read"}

	uri := ""
	cmd.PersistentFlags().StringVar(&uri, "uri", uri, "kfile uri")

	cmd.RunE = func(*cobra.Command, []string) error {
		wg := sync.WaitGroup{}
		for j := 0; j < concurrent; j++ {
			wg.Add(1)
			go func(ind int) {
				defer wg.Done()
				reader, err := kfile.NewReader(uri, client)
				if err != nil {
					fmt.Println(fmt.Errorf("new reader: %w", err))
					return
				}
				buf, err := reader.ReadAll(0)
				if err != nil {
					fmt.Println(fmt.Errorf("read: %w", err))
					return
				}
				fmt.Printf("read from %s\n", uri)
				fmt.Println(buf.String())
				if _, err := reader.Close(0); err != nil {
					fmt.Println(err)
					return
				}
			}(j)
		}
		wg.Wait()
		return nil
	}
	return &cmd
}
