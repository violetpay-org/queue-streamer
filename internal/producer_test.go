package internal_test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
	"testing"
)

var pbrokers = []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}

func TestProducerPool_Take(t *testing.T) {
	pool := internal.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	topic := shared.NewTopic("test", 3)

	producer := pool.Take(topic)
	assert.NotNil(t, &producer)

	producers := pool.Producers()
	assert.NotNil(t, &producers)

	assert.Equal(t, 0, len(producers[topic]))
}

func TestProducerPool_Return(t *testing.T) {
	pool := internal.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	topic := shared.NewTopic("test", 3)

	producer := pool.Take(topic)
	assert.NotNil(t, &producer)

	pool.Return(producer, topic)

	producers := pool.Producers()
	assert.NotNil(t, &producers)

	assert.Equal(t, 1, len(producers[topic]))

	pool.Return(nil, topic)
	assert.Equal(t, 1, len(producers[topic]))

	producer = pool.Take(topic)
	assert.Equal(t, 0, len(producers[topic]))
}
