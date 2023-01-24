package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	kf "github.com/confluentinc/confluent-kafka-go/kafka"
)

func (self *kafkaImpl) Produce(
	topic, value string,
) (*kf.Message, error) {
	delivery_chan := make(chan kf.Event, 10000)
	defer close(delivery_chan)

	if self.producer == nil {
		debugObjects := ""

		if self.debug {
			debugObjects = "broker, topic, metadata"
		}

		producer, err := kf.NewProducer(&kf.ConfigMap{
			"bootstrap.servers":       strings.Join(self.servers, ","),
			"acks":                    "all",
			"retries":                 "0",
			"request.timeout.ms":      fmt.Sprintf("%d", self.timeout),
			"socket.keepalive.enable": "true",
			"debug":                   debugObjects,
		})

		if err != nil {
			return nil, err
		}

		self.producer = producer
	}

	ctx, cancel := context.WithTimeout(context.Background(), self.timeout*time.Millisecond)
	defer cancel()

	err := self.producer.Produce(&kf.Message{
		TopicPartition: kf.TopicPartition{
			Topic:     &topic,
			Partition: kf.PartitionAny,
		},
		Value: []byte(value),
	}, delivery_chan)

	if err != nil {
		return nil, err
	}

	select {
	case resp := <-delivery_chan:
		msg := resp.(*kf.Message)

		fmt.Println(msg)

		if msg.TopicPartition.Error != nil {
			return nil, msg.TopicPartition.Error
		}
		return msg, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
