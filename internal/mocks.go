package internal

import (
	"context"
	"github.com/IBM/sarama"
)

// MockConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type MockConsumerGroupSession struct {
	Ctx               context.Context
	ResetOffsetCalled bool
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
	t.ResetOffsetCalled = true
	return
}

func (t *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	return
}

func (t *MockConsumerGroupSession) Context() context.Context {
	return t.Ctx
}

// MockConsumerGroupClaim is a mock implementation of sarama.ConsumerGroupClaim
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

// MockAsyncProducer is a mock implementation of sarama.AsyncProducer
type MockAsyncProducer struct {
	TxnStatusFlag  sarama.ProducerTxnStatusFlag
	AbortTxnCalled bool
}

func (t *MockAsyncProducer) AsyncClose() {
	return
}

func (t *MockAsyncProducer) Close() error {
	return nil
}

func (t *MockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return nil
}

func (t *MockAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return nil
}

func (t *MockAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return nil
}

func (t *MockAsyncProducer) IsTransactional() bool {
	return false
}

func (t *MockAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return t.TxnStatusFlag
}

func (t *MockAsyncProducer) BeginTxn() error {
	return nil
}

func (t *MockAsyncProducer) CommitTxn() error {
	return nil
}

func (t *MockAsyncProducer) AbortTxn() error {
	t.AbortTxnCalled = true
	return nil
}

func (t *MockAsyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (t *MockAsyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}
