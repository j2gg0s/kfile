# kfile

Use kafka for distribute file storage, support write append and auto expire.

# User story

In our team, we use PostgreSQL/MySQL as data storage, Kafka as message queue.
It's difficult to share file between multi instances under these components.

For use, sharing file is a rare situation, only like export thoundes of records.
We don't want to import one Distributed File System, so we use kafka to save temporary file.

# Usage

Write:

```go
name := "fileA"
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
```

Read:

```go.
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
```

See more in `example/`
