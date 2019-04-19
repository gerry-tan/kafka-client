package kafka_client

import (
	"fmt"
	"github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"testing"
	"time"
)

var (
	hosts   = []string{"localhost:9092"}
	topic   = "TestA"
	groupId = "test1234"
)

// kafka 生产者
func TestPutMsg(t *testing.T) {
	InitProducer(hosts)
	PutMsg(topic, []byte("hello world!"), "1")
	time.Sleep(1 * time.Second)
}

// kafka 消费测试
func TestNewGroupConsumer(t *testing.T) {
	group := NewGroupConsumer(hosts, []string{topic}, groupId, nil)

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case pc := <-group.Partitions():
			go handlePartition(pc)
		case <-signals:
			CloseConsumer()
			return
		}
	}
}

func handlePartition(pc cluster.PartitionConsumer) {
	for msg := range pc.Messages() {
		fmt.Printf("topic: %s, offset: %d, value: %v, key: %v\n", msg.Topic, msg.Offset, string(msg.Value), string(msg.Key))
		pc.MarkOffset(msg.Offset, "") //提交
	}
}
