package internal_test

import (
	"context"
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
	origin := shared.NewTopic("test", 3)
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	consumer.AddDestination(shared.NewTopic("test2", 3), &TestSerializer{})
	assert.Equal(t, 1, len(consumer.Destinations()))
	assert.Equal(t, 1, len(consumer.MessageSerializers()))

	consumer.AddDestination(shared.NewTopic("test3", 3), &TestSerializer{})
	assert.Equal(t, 2, len(consumer.Destinations()))
	assert.Equal(t, 2, len(consumer.MessageSerializers()))

	assert.NotEqual(t, consumer.Destinations()[0], consumer.Destinations()[1])
}

// MockConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type MockConsumerGroupSession struct {
	ctx context.Context
}

func (t *MockConsumerGroupSession) Claims() map[string][]int32 {
	return nil
}

func (t *MockConsumerGroupSession) MemberID() string {
	return ""
}

func (t *MockConsumerGroupSession) GenerationID() int32 {
	return 0
}

func (t *MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	return
}

func (t *MockConsumerGroupSession) Commit() {
	return
}

func (t *MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	return
}

func (t *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	return
}

func (t *MockConsumerGroupSession) Context() context.Context {
	return t.ctx
}

func TestStreamConsumer_Setup(t *testing.T) {
	sess := &MockConsumerGroupSession{}
	origin := shared.NewTopic("test", 3)
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	consumer.AddDestination(shared.NewTopic("test2", 3), &TestSerializer{})
	assert.Equal(t, 1, len(consumer.Destinations()))

	assert.NotPanics(t, func() {
		_ = consumer.Setup(sess)
	})

	consumer = &internal.StreamConsumer{}
	assert.Panics(t, func() {
		_ = consumer.Setup(sess)
	})
}

type MockConsumerGroupClaim struct {
	DataChan chan *sarama.ConsumerMessage
}

func (t *MockConsumerGroupClaim) Topic() string {
	return ""
}

func (t *MockConsumerGroupClaim) Partition() int32 {
	return 0
}

func (t *MockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (t *MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (t *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return t.DataChan
}

func TestStreamConsumer_ConsumeClaim(t *testing.T) {
	origin := shared.NewTopic("test", 3)
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	//producerPool := internal.NewProducerPool(cbrokers, func() *sarama.Config {
	//	return sarama.NewConfig()
	//})

	//producer := producerPool.Take(origin)

	t.Run("ConsumeClaim Gracefully shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sess := &MockConsumerGroupSession{ctx: ctx}

		exited := false
		mutex := &sync.Mutex{}
		go func() {
			_ = consumer.ConsumeClaim(sess, &MockConsumerGroupClaim{})
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
		sess := &MockConsumerGroupSession{ctx: ctx}

		msg := &MockConsumerGroupClaim{
			DataChan: make(chan *sarama.ConsumerMessage, 1),
		}

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
		time.Sleep(5 * time.Second)

		mutex.Lock()
		assert.True(t, exited)
		mutex.Unlock()
	})
}
