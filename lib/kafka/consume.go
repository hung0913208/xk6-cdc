package kafka

import (
	"errors"
	"fmt"
	"strings"

	kf "github.com/confluentinc/confluent-kafka-go/kafka"
)

type consumerController struct {
	Running bool `json:"running"`
}

func (self *kafkaImpl) Consume() (*consumerController, error) {
	controller := &consumerController{}

	if self.consumer == nil {
		debugObjects := ""

		if self.debug {
			debugObjects = "broker, topic, metadata"
		}

		consumer, err := kf.NewConsumer(&kf.ConfigMap{
			"bootstrap.servers":               strings.Join(self.servers, ","),
			"group.id":                        "",
			"auto.offset.reset":               "smallet",
			"go.application.rebalance.enable": true,
			"debug":                           debugObjects,
		})
		if err != nil {
			return nil, err
		}

		self.consumer = consumer
	}

	go self.handleKafkaMessages(controller, self.topics)
	return controller, nil
}

func (self *kafkaImpl) handleKafkaMessages(
	controller *consumerController,
	topics []string,
) {
	err := self.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		self.Throw(err)
	}

	for controller.Running {
		ev := self.consumer.Poll(int(self.timeout))

		switch e := ev.(type) {
		case *kf.Message:
		case kf.PartitionEOF:
		case kf.Error:
			self.Throw(errors.New(fmt.Sprintf("Error: %v", e)))
		default:
			self.Throw(errors.New(fmt.Sprintf("Error: %v", e)))
		}
	}
}
