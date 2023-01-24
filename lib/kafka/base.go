package kafka

import (
	"errors"
	"reflect"
	"time"

	kf "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Kafka interface {
	Open(
		servers, topics []string,
		timeout int,
		debug bool,
	) error
	Consume() (*consumerController, error)
	Produce(topic, value string) (*kf.Message, error)
	Throw(err error)
	Close() error
}

type kafkaImpl struct {
	consumer *kf.Consumer
	producer *kf.Producer
	servers  []string
	topics   []string
	running  bool
	timeout  time.Duration
	debug    bool
}

func NewKafka() Kafka {
	return &kafkaImpl{}
}

func (self *kafkaImpl) Throw(err error) {
}

func (self *kafkaImpl) Open(
	servers, topics []string,
	timeout int,
	debug bool,
) error {
	if self.consumer != nil || self.producer != nil {
		if !reflect.DeepEqual(self.servers, servers) ||
			!reflect.DeepEqual(self.topics, topics) {
			self.Close()
		}
	}

	self.timeout = time.Duration(timeout)
	self.servers = servers
	self.topics = topics
	self.debug = debug
	return nil
}

func (self *kafkaImpl) Close() error {
	err := errors.New("kafka connection has been closed")

	if self.consumer != nil {
		self.consumer.Close()
		err = nil
	}

	if self.consumer != nil {
		self.producer.Close()
		err = nil
	}

	if err == nil {
		self.consumer = nil
		self.producer = nil
	}
	return err
}
