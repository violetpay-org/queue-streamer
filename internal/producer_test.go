package internal_test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
	"testing"
)

var brokers = []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}

func TestProducerPool_Take(t *testing.T) {
	pool := internal.NewProducerPool(brokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	topic := shared.NewTopic("test", 3)

	producer := pool.Take(topic)
	assert.NotNil(t, &producer)
}

func TestProducerPool_Return(t *testing.T) {
	pool := internal.NewProducerPool(brokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	topic := shared.NewTopic("test", 3)

	producer := pool.Take(topic)
	assert.NotNil(t, &producer)

	pool.Return(producer, topic)

	producer = pool.Take(topic)
	assert.NotNil(t, &producer)
}
