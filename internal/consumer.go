package internal

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/shared"
)

// StreamConsumer is a consumer that consumes messages from a Kafka topic and produces them to other topics.
// It implements the sarama.ConsumerGroupHandler interface.
type StreamConsumer struct {
	groupId      string
	conn         sarama.Client
	producerPool *producerPool
	destinations []shared.Topic
	mss          []shared.MessageSerializer
}

func NewStreamConsumer(destinations []shared.Topic, messageSerializers []shared.MessageSerializer, groupId string, conn sarama.Client) *StreamConsumer {
	return &StreamConsumer{
		groupId:      groupId,
		producerPool: newProducerPool(conn),
		destinations: destinations,
		mss:          messageSerializers,
	}
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

			func() {
				topic := shared.NewTopic(msg.Topic, msg.Partition)

				producer := consumer.producerPool.Take(topic)
				defer consumer.producerPool.Return(producer, topic)

				// Start a new transaction
				err := producer.BeginTxn()
				if err != nil {
					fmt.Println("Error starting transaction:", err)
					return
				}

				for i, destination := range consumer.destinations {
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
