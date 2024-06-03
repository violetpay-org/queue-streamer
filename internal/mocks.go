package internal

import (
	"context"
	"github.com/IBM/sarama"
)

// MockConsumerGroupSession is a mock implementation of sarama.ConsumerGroupSession
type MockConsumerGroupSession struct {
	Ctx               context.Context
	ResetOffsetCalled int
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
	t.ResetOffsetCalled++
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
	TxnStatusFlag         sarama.ProducerTxnStatusFlag
	AbortTxnCalled        int
	AbortTxnError         error
	BeginTxnCalled        int
	BeginTxnError         error
	CommitTxnCalled       int
	CommitTxnError        error
	AddMessageToTxnCalled int
	AddMessageToTxnError  error
	CloseCalled           int
	InputChan             chan *sarama.ProducerMessage
}

func (t *MockAsyncProducer) AsyncClose() {
	return
}

func (t *MockAsyncProducer) Close() error {
	t.CloseCalled++
	return nil
}

func (t *MockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return t.InputChan
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
	t.BeginTxnCalled++
	if t.BeginTxnError != nil && t.BeginTxnCalled < 10 {
		return t.BeginTxnError
	}

	return nil
}

func (t *MockAsyncProducer) CommitTxn() error {
	t.CommitTxnCalled++
	if t.CommitTxnError != nil && t.CommitTxnCalled < 10 {
		return t.CommitTxnError
	}
	return nil
}

func (t *MockAsyncProducer) AbortTxn() error {
	t.AbortTxnCalled++
	if t.AbortTxnError != nil && t.AbortTxnCalled < 10 {
		return t.AbortTxnError
	}
	return nil
}

func (t *MockAsyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}

func (t *MockAsyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	t.AddMessageToTxnCalled++
	if t.AddMessageToTxnError != nil && t.AddMessageToTxnCalled < 10 {
		return t.AddMessageToTxnError
	}
	return nil
}

type MockConsumerGroupHandler struct {
	SetupCalled        int
	CleanupCalled      int
	ConsumeClaimCalled int
	ConsumeClaimError  error
}

func (t *MockConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	t.SetupCalled++
	return nil
}

func (t *MockConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	t.CleanupCalled++
	return nil

}

func (t *MockConsumerGroupHandler) ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error {
	t.ConsumeClaimCalled++
	if t.ConsumeClaimError != nil {
		return t.ConsumeClaimError
	}

	return nil
}
