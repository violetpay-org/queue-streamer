package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/shared"
	"sync"
	"time"
)

var transactionalId int32 = 0
var mutex = &sync.Mutex{}

// StreamConsumer is a consumer that consumes messages from a Kafka topic and produces them to other topics.
// It implements the sarama.ConsumerGroupHandler interface.
type StreamConsumer struct {
	groupId      string
	producerPool *producerPool
	origin       shared.Topic
	dests        []shared.Topic
	mss          []shared.MessageSerializer

	// For kafka
	brokers []string
	config  *sarama.Config
}

func NewStreamConsumer(
	origin shared.Topic, groupId string,
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
		dests:        make([]shared.Topic, 0),
		mss:          make([]shared.MessageSerializer, 0),
		brokers:      brokers,
		config:       config,
	}
}

func (consumer *StreamConsumer) AddDestination(dest shared.Topic, serializer shared.MessageSerializer) {
	consumer.dests = append(consumer.dests, dest)
	consumer.mss = append(consumer.mss, serializer)
}

func (consumer *StreamConsumer) StartAsGroup(ctx context.Context) {
	client, err := sarama.NewConsumerGroup(consumer.brokers, consumer.groupId, consumer.config)
	if err != nil {
		panic(err)
	}

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := client.Consume(ctx, []string{consumer.origin.Name()}, consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			fmt.Println("Error from consumer: ", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			fmt.Println("Context cancelled")
			return
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *StreamConsumer) Setup(session sarama.ConsumerGroupSession) error {
	if len(consumer.dests) != len(consumer.mss) {
		panic("Number of message serializers must match number of dests")
	}

	if len(consumer.brokers) < 1 {
		panic("No brokers")
	}

	if consumer.config == nil {
		panic("No config")
	}

	if consumer.groupId == "" {
		consumer.groupId = "streamer"
	}

	if consumer.producerPool == nil {
		panic("No producer pool")
	}

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

			func() {
				topic := shared.NewTopic(msg.Topic, msg.Partition)

				producer := consumer.producerPool.Take(topic)
				defer consumer.producerPool.Return(producer, topic)

				// Start a new transaction
				err := producer.BeginTxn()
				if err != nil {
					fmt.Println("Error starting transaction:", err)
					consumer.handleTxnError(producer, msg, session, err, func() error {
						return producer.BeginTxn()
					})
					return
				}

				for i, destination := range consumer.dests {
					// Produce the message
					producer.Input() <- &sarama.ProducerMessage{
						Topic: destination.Name(),
						Value: sarama.ByteEncoder(
							consumer.mss[i].MessageToProduceMessage(string(msg.Value)),
						),
					}

					fmt.Println("Message produced:", destination.Name(), msg.Key, msg.Value)
				}

				// Add the message to the transaction
				err = producer.AddMessageToTxn(msg, consumer.groupId, nil)
				if err != nil {
					fmt.Println("Error adding message to transaction:", err)
					consumer.handleTxnError(producer, msg, session, err, func() error {
						return producer.AddMessageToTxn(msg, consumer.groupId, nil)
					})
					return
				}

				// Commit the transaction
				err = producer.CommitTxn()
				if err != nil {
					fmt.Println("Error committing transaction:", err)
					consumer.handleTxnError(producer, msg, session, err, func() error {
						return producer.AddMessageToTxn(msg, consumer.groupId, nil)
					})
					return
				}

				fmt.Println("Message claimed:", msg.Topic, msg.Partition, msg.Offset)
			}()
		case <-session.Context().Done():
			return nil
		}
	}
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
