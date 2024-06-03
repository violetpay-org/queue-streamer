package internal_test

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
	"sync"
	"testing"
	"time"
)

var cbrokers = []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}

// TestSerializer is a mock implementation of shared.MessageSerializer
type TestSerializer struct {
}

func (ts *TestSerializer) MessageToProduceMessage(value string) string {
	return value
}

func TestStreamConsumer_AddDestination(t *testing.T) {
	origin := shared.Topic{Name: "test", Partition: 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	t.Cleanup(func() {})

	t.Run("AddDestination", func(t *testing.T) {
		consumer.AddDestination(shared.Topic{Name: "test2", Partition: 3}, &TestSerializer{})
		assert.Equal(t, 1, len(consumer.Destinations()))
		assert.Equal(t, 1, len(consumer.MessageSerializers()))

		consumer.AddDestination(shared.Topic{Name: "test3", Partition: 3}, &TestSerializer{})
		assert.Equal(t, 2, len(consumer.Destinations()))
		assert.Equal(t, 2, len(consumer.MessageSerializers()))

		assert.NotEqual(t, consumer.Destinations()[0], consumer.Destinations()[1])
	})
}

func TestStreamConsumer_Setup(t *testing.T) {
	origin := shared.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	sess := &internal.MockConsumerGroupSession{}

	t.Cleanup(func() {
		sess = &internal.MockConsumerGroupSession{}
	})

	t.Run("Setup", func(t *testing.T) {
		consumer.AddDestination(shared.Topic{"test2", 3}, &TestSerializer{})
		assert.Equal(t, 1, len(consumer.Destinations()))

		assert.NotPanics(t, func() {
			_ = consumer.Setup(sess)
		})

		consumer = &internal.StreamConsumer{}
		assert.Panics(t, func() {
			_ = consumer.Setup(sess)
		})
	})
}

func TestStreamConsumer_ConsumeClaim(t *testing.T) {
	origin := shared.Topic{Name: "test", Partition: 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	sess := &internal.MockConsumerGroupSession{}
	msg := &internal.MockConsumerGroupClaim{}

	//producerPool := internal.NewProducerPool(cbrokers, func() *sarama.Config {
	//	return sarama.NewConfig()
	//})

	//producer := producerPool.Take(origin)

	t.Cleanup(func() {
		sess = &internal.MockConsumerGroupSession{}
		msg = &internal.MockConsumerGroupClaim{}
	})

	t.Run("ConsumeClaim Gracefully shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sess.Ctx = ctx

		exited := false
		mutex := &sync.Mutex{}
		go func() {
			_ = consumer.ConsumeClaim(sess, msg)
			mutex.Lock()
			defer mutex.Unlock()
			exited = true
		}()

		cancel()
		time.Sleep(100 * time.Millisecond)
		mutex.Lock()
		assert.True(t, exited)
		mutex.Unlock()
	})

	t.Run("ConsumeClaim Consume message", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)
		sess.Ctx = ctx

		exited := false
		mutex := &sync.Mutex{}
		go func() {
			assert.Equal(t, 0, len(consumer.ProducerPool().Producers()))
			_ = consumer.ConsumeClaim(sess, msg)

			mutex.Lock()
			exited = true
			mutex.Unlock()
			assert.Equal(t, 1, len(consumer.ProducerPool().Producers()))
		}()

		time.Sleep(1 * time.Second)

		msg.DataChan <- &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Key:       []byte("key"),
			Value:     []byte("value"),
			Offset:    0,
		}

		cancel()
		time.Sleep(2 * time.Second)

		mutex.Lock()
		assert.True(t, exited)
		mutex.Unlock()
	})
}

func TestHandleTxnError(t *testing.T) {
	origin := shared.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	producer := &internal.MockAsyncProducer{}
	message := &sarama.ConsumerMessage{}
	session := &internal.MockConsumerGroupSession{}

	t.Cleanup(func() {
		session = &internal.MockConsumerGroupSession{}
		producer = &internal.MockAsyncProducer{}
		message = &sarama.ConsumerMessage{}
	})

	t.Run("HandleTxnError with error", func(t *testing.T) {
		functionCalledCount := 0
		testFunction := func() {
			functionCalledCount++
		}

		consumer.HandleTxnError(producer, message, session, nil, func() error {
			testFunction()
			return nil
		})
		assert.False(t, session.ResetOffsetCalled)
		assert.False(t, producer.AbortTxnCalled)

		assert.Equal(t, 1, functionCalledCount)
	})

	t.Run("HandleTxnError with error, called defaulthandler function several times for retry", func(t *testing.T) {
		functionCalledCount := 0
		testFunction := func() error {
			functionCalledCount++

			if functionCalledCount == 10 {
				return nil
			}

			return errors.New("error")
		}

		consumer.HandleTxnError(producer, message, session, nil, testFunction)
		assert.False(t, session.ResetOffsetCalled)
		assert.False(t, producer.AbortTxnCalled)

		assert.Equal(t, 10, functionCalledCount)
	})

	t.Run("HandleTxnError with ProducerTxnFlagInError", func(t *testing.T) {
		producer.TxnStatusFlag = sarama.ProducerTxnFlagFatalError

		consumer.HandleTxnError(producer, message, session, nil, func() error {
			return nil
		})
		assert.True(t, session.ResetOffsetCalled)
		assert.False(t, producer.AbortTxnCalled)
	})

	t.Run("HandleTxnError with ProducerTxnFlagAbortableError", func(t *testing.T) {
		producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

		consumer.HandleTxnError(producer, message, session, nil, func() error {
			return nil
		})
		assert.True(t, session.ResetOffsetCalled)
		assert.True(t, producer.AbortTxnCalled)
	})

}
