package kafka_client

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"log"
	"time"
)

var consumer *cluster.Consumer

func initConfig() (config *cluster.Config) {
	// init (custom) config, enable errors and notifications
	config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //初始消费的offset位置
	config.Group.Return.Notifications = true
	config.Group.Mode = cluster.ConsumerModePartitions
	return
}

func NewGroupConsumer(brokers []string, topics []string, groupId string, config *cluster.Config) *cluster.Consumer {
	if config == nil {
		config = initConfig()
	}

	// init consumer
	var err error
	consumer, err = cluster.NewConsumer(brokers, groupId, topics, config)
	if err != nil {
		log.Fatal(err)
	}

	go handleErrors()
	go handleNtf()

	return consumer
}

func handleErrors() {
	for err := range consumer.Errors() {
		log.Printf("consumer message error: %s\n", err.Error())
	}
}

func handleNtf() {
	for ntf := range consumer.Notifications() {
		log.Printf("consumer Rebalanced: %+v\n", ntf)
	}
}

func CloseConsumer() {
	defer consumer.Close()
}
