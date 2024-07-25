package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/common"
	"sync"
	"time"
)

var transactionalId int32 = 0
var mutex = &sync.Mutex{}

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
) *StreamConsumer {
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

		mutex.Lock()
		defer mutex.Unlock()
		pcfg.Producer.Transaction.ID = pcfg.Producer.Transaction.ID + fmt.Sprintf("-%d", transactionalId)
		transactionalId++

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
	client, err := sarama.NewConsumerGroup(consumer.brokers, consumer.groupId, consumer.config)
	if err != nil {
		panic(err)
	}

	consumer.groupMutex.Lock()
	consumer.consumerGroup = client
	consumer.groupMutex.Unlock()

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := client.Consume(ctx, []string{consumer.origin.Name}, handler); err != nil {
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

				consumer.Transaction(producer, msg, session)
			}()

		case <-session.Context().Done():
			return nil
		}
	}
}

func (consumer *StreamConsumer) Transaction(producer sarama.AsyncProducer, message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	err := producer.BeginTxn()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		consumer.handleTxnError(producer, message, session, err, func() error {
			return producer.BeginTxn()
		})
		return
	}

	for i, destination := range consumer.dests {
		// Produce the message
		producer.Input() <- &sarama.ProducerMessage{
			Topic: destination.Name,
			Value: sarama.ByteEncoder(
				consumer.mss[i].MessageToProduceMessage(string(message.Value)),
			),
		}

		fmt.Println("Message produced:", destination.Name, message.Key, message.Value)
	}

	// Add the message to the transaction
	err = producer.AddMessageToTxn(message, consumer.groupId, nil)
	if err != nil {
		fmt.Println("Error adding message to transaction:", err)
		consumer.handleTxnError(producer, message, session, err, func() error {
			return producer.AddMessageToTxn(message, consumer.groupId, nil)
		})
		return
	}

	// Commit the transaction
	err = producer.CommitTxn()
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		consumer.handleTxnError(producer, message, session, err, func() error {
			return producer.CommitTxn()
		})
		return
	}

	fmt.Println("Message claimed:", message.Topic, message.Partition, message.Offset)
}

// HandleTxnError handles transaction errors, this exported method is only for testing purposes.
func (consumer *StreamConsumer) HandleTxnError(producer sarama.AsyncProducer, message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, err error, defaulthandler func() error) {
	consumer.handleTxnError(producer, message, session, err, defaulthandler)
}

func (consumer *StreamConsumer) handleTxnError(producer sarama.AsyncProducer, message *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, err error, defaulthandler func() error) {
	fmt.Printf("Message consumer: unable to process transaction: %+v", err)
	for {
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			fmt.Println("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			err = producer.AbortTxn()
			if err != nil {
				fmt.Printf("Message consumer: unable to abort transaction: %+v", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		// if not you can retry
		err = defaulthandler()
		if err == nil {
			return
		}
	}
}
