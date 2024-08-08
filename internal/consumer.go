package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/violetpay-org/queue-streamer/common"
)

type IStreamConsumer interface {
	sarama.ConsumerGroupHandler
	AddDestination(dest common.Topic, serializer common.MessageSerializer)
	Destinations() []common.Topic
	MessageSerializers() []common.MessageSerializer
	ProducerPool() *ProducerPool
	StartAsGroup(ctx context.Context, handler sarama.ConsumerGroupHandler)
	StartAsGroupSelf(ctx context.Context)
	CloseGroup()
}

// StreamConsumer is a consumer that consumes messages from a Kafka topic and produces them to other topics.
// It implements the sarama.ConsumerGroupHandler interface.
type StreamConsumer struct {
	groupId      string
	producerPool *ProducerPool
	origin       common.Topic
	dests        []common.Topic
	mss          []common.MessageSerializer

	// For kafka
	brokers       []string
	config        *sarama.Config
	consumerGroup sarama.ConsumerGroup
	groupMutex    *sync.Mutex
}

func (consumer *StreamConsumer) ProducerPool() *ProducerPool {
	return consumer.producerPool
}

func NewStreamConsumer(
	origin common.Topic, groupId string,
	brokers []string, config *sarama.Config, producerConfig *sarama.Config,
) IStreamConsumer {
	if config == nil {
		config = sarama.NewConfig()
	}

	if producerConfig == nil {
		producerConfig = sarama.NewConfig()
	}

	// producerConfigProvider is for transactional producer.
	producerConfigProvider := func() *sarama.Config {
		var pcfg *sarama.Config = sarama.NewConfig()

		// Deep copy for preventing race condition
		Copy(producerConfig, pcfg)

		// override the configuration
		pcfg.Net.MaxOpenRequests = 1
		pcfg.Producer.Idempotent = true
		pcfg.Producer.RequiredAcks = sarama.WaitForAll
		pcfg.Producer.Retry.Max = 5
		pcfg.Producer.Retry.Backoff = 1000 * time.Millisecond

		if pcfg.Producer.Transaction.ID == "" {
			pcfg.Producer.Transaction.ID = "streamer"
		}

		pcfg.Producer.Transaction.ID = fmt.Sprintf("%s-%s", pcfg.Producer.Transaction.ID, uuid.New())

		return pcfg
	}

	// override the configuration
	config.Consumer.Offsets.AutoCommit.Enable = false

	return &StreamConsumer{
		groupId:      groupId,
		producerPool: NewProducerPool(brokers, producerConfigProvider),
		origin:       origin,
		dests:        make([]common.Topic, 0),
		mss:          make([]common.MessageSerializer, 0),
		brokers:      brokers,
		config:       config,
		groupMutex:   &sync.Mutex{},
	}
}

func (consumer *StreamConsumer) AddDestination(dest common.Topic, serializer common.MessageSerializer) {
	consumer.dests = append(consumer.dests, dest)
	consumer.mss = append(consumer.mss, serializer)
}

func (consumer *StreamConsumer) Destinations() []common.Topic {
	return consumer.dests
}

func (consumer *StreamConsumer) MessageSerializers() []common.MessageSerializer {
	return consumer.mss
}

func (consumer *StreamConsumer) startAsGroup(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	consumerGroup, err := sarama.NewConsumerGroup(consumer.brokers, consumer.groupId, consumer.config)
	if err != nil {
		panic(err)
	}

	consumer.groupMutex.Lock()
	consumer.consumerGroup = consumerGroup
	consumer.groupMutex.Unlock()

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session should be
		// recreated to get the new claims
		if err := consumerGroup.Consume(ctx, []string{consumer.origin.Name}, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			fmt.Println("Context cancelled")
			return
		}
	}
}

func (consumer *StreamConsumer) StartAsGroup(ctx context.Context, handler sarama.ConsumerGroupHandler) {
	consumer.startAsGroup(ctx, handler)
}

func (consumer *StreamConsumer) StartAsGroupSelf(ctx context.Context) {
	consumer.startAsGroup(ctx, consumer)
}

func (consumer *StreamConsumer) CloseGroup() {
	consumer.groupMutex.Lock()
	if consumer.consumerGroup != nil {
		consumer.consumerGroup.Close()
	}
	consumer.groupMutex.Unlock()
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *StreamConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (consumer *StreamConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// NOTE: This must not be called within a goroutine, already handled as goroutines by sarama.
func (consumer *StreamConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("Message consumer: starting to consume messages")
	transactionMaker := NewRetriableTransactionMaker(
		session,
		consumer.groupId,
	)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// Message channel is closed, return
				return nil
			}

			topic := common.Topic{
				Name:      msg.Topic,
				Partition: msg.Partition,
			}

			func() {
				producer := consumer.producerPool.Take(topic)
				defer consumer.producerPool.Return(producer, topic)

				transactionMaker.AsyncTransact(
					producer,
					msg,
					consumer.dests,
					consumer.mss,
				)
			}()

		case <-session.Context().Done():
			return nil
		}
	}
}
