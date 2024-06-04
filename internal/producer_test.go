package internal_test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
	"testing"
)

var pbrokers = []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}

func TestNewProducerPool(t *testing.T) {
	t.Run("NewProducerPool no configProvider", func(t *testing.T) {
		assert.Panics(t, func() {
			_ = internal.NewProducerPool(pbrokers, nil)
		})

		assert.Panics(t, func() {
			_ = internal.NewProducerPool(pbrokers, func() *sarama.Config {
				return nil
			})
		})
	})
}

func TestProducerPool_Close(t *testing.T) {
	pool := internal.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	topic := shared.Topic{Name: "test", Partition: 3}

	producer := pool.Take(topic)
	assert.NotNil(t, &producer)

	pool.Return(producer, topic)

	assert.NotPanics(t, func() {
		pool.Close()
	})
}

func TestProducerPool_Take(t *testing.T) {
	topic := shared.Topic{Name: "test", Partition: 3}
	pool := internal.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})
	var producer sarama.AsyncProducer

	t.Run("Take", func(t *testing.T) {
		t.Cleanup(func() {
			pool = internal.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		producer = pool.Take(topic)
		assert.NotNil(t, &producer)

		producers := pool.Producers()
		assert.NotNil(t, &producers)

		assert.Equal(t, 0, len(producers[topic]))
	})

	t.Run("Take with error when generate producer", func(t *testing.T) {
		t.Cleanup(func() {
			pool = internal.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		pool = internal.NewProducerPool(pbrokers, func() *sarama.Config {
			return &sarama.Config{}
		})

		producer = pool.Take(topic)
		assert.Nil(t, producer)
	})
}

func TestProducerPool_Return(t *testing.T) {
	topic := shared.Topic{Name: "test", Partition: 3}
	pool := internal.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	t.Run("Return", func(t *testing.T) {
		t.Cleanup(func() {
			pool = internal.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

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
	})

	//t.Run("Return closed producer", func(t *testing.T) {
	//	assert.Equal(t, 0, len(producers[topic]))
	//	producer = pool.Take(topic)
	//	err := producer.Close()
	//	assert.Nil(t, err)
	//	err = producer.Close()
	//	assert.Nil(t, err)
	//	producer.Errors()
	//
	//	pool.Return(producer, topic)
	//	assert.Equal(t, 0, len(producers[topic]))
	//})

	t.Run("Return txError producer", func(t *testing.T) {
		t.Cleanup(func() {
			pool = internal.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		producer := &internal.MockAsyncProducer{TxnStatusFlag: sarama.ProducerTxnFlagInError}
		producers := pool.Producers()

		pool.Return(producer, topic)
		assert.Equal(t, 0, len(producers[topic]))
		assert.Equal(t, 1, producer.CloseCalled)
	})
}
