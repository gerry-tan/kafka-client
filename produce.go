package kafka_client

import (
	"github.com/Shopify/sarama"
	"log"
)

var (
	producer sarama.AsyncProducer
)

func InitProducer(kafkaAddrs []string) {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForLocal // Only wait for the leader to ack
	conf.Producer.Partitioner = sarama.NewHashPartitioner
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true

	var err error
	producer, err = sarama.NewAsyncProducer(kafkaAddrs, conf)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			select {
			case pm := <-producer.Successes():
				log.Printf("producer message success, partition:%d offset:%d Topic:%v ", pm.Partition, pm.Offset, pm.Topic)
			case fail := <-producer.Errors():
				log.Printf("producer message error, partition:%d offset:%d Topic:%v values:%s error(%v)", fail.Msg.Partition, fail.Msg.Offset, fail.Msg.Topic, fail.Msg.Value, fail.Err)
			}
		}
	}()
	return
}

func CloseProduce() {
	producer.AsyncClose()
}

func PutMsg(queue string, data []byte, key string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: queue, Value: sarama.ByteEncoder(data), Key: sarama.ByteEncoder(key)}
	return
}
