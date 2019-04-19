package kafka_client

import (
	"fmt"
	"testing"
	"time"
)

var (
	hosts   = []string{"localhost:9092"}
	topic   = "TestA"
	groupId = "test1234"
)

func TestPutMsg(t *testing.T) {
	InitProducer(hosts)
	PutMsg(topic, []byte("hello world!"), "1")
	time.Sleep(1 * time.Second)
}

func TestNewGroupConsumer(t *testing.T) {
	group := NewGroupConsumer(hosts, []string{topic}, groupId, nil)

	for pc := range group.Partitions() {
		go func() {
			for msg := range pc.Messages() {
				fmt.Printf("topic: %s, offset: %d, value: %v, key: %v\n", msg.Topic, msg.Offset, string(msg.Value), string(msg.Key))
				pc.MarkOffset(msg.Offset, "")
			}
		}()
	}
}
