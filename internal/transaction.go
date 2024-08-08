package internal

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/common"
)

type ITransactionMaker interface {
	SyncTransact(
		producer sarama.SyncProducer,
		message *sarama.ConsumerMessage,
		destinationTopics []common.Topic,
		destinationMessageSerializers []common.MessageSerializer,
	) error

	AsyncTransact(
		producer sarama.AsyncProducer,
		message *sarama.ConsumerMessage,
		destinationTopics []common.Topic,
		destinationMessageSerializers []common.MessageSerializer,
	) error
}

func NewRetriableTransactionMaker(
	session sarama.ConsumerGroupSession,
	consumerGroupId string,
) ITransactionMaker {
	return &retriableTransactionMaker{
		session:         session,
		consumerGroupId: consumerGroupId,
	}
}

type retriableTransactionMaker struct {
	session         sarama.ConsumerGroupSession
	consumerGroupId string
}

func (rtm *retriableTransactionMaker) SyncTransact(
	producer sarama.SyncProducer,
	message *sarama.ConsumerMessage,
	destinationTopics []common.Topic,
	destinationMessageSerializers []common.MessageSerializer,
) error {
	return errors.New("not implemented")
}

func (rtm *retriableTransactionMaker) AsyncTransact(
	producer sarama.AsyncProducer,
	message *sarama.ConsumerMessage,
	destinationTopics []common.Topic,
	destinationMessageSerializers []common.MessageSerializer,
) error {
	err := producer.BeginTxn()
	if err != nil {
		fmt.Println("Error starting transaction:", err)
		rtm.handleTxnError(producer, message, err, func() error {
			return producer.BeginTxn()
		})
		return nil
	}

	for i, topic := range destinationTopics {
		// Produce the message
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic.Name,
			Value: sarama.ByteEncoder(
				destinationMessageSerializers[i].MessageToProduceMessage(string(message.Value)),
			),
		}

		fmt.Println("Message produced:", topic.Name, message.Key, message.Value)
	}

	// Add the message to the transaction
	err = producer.AddMessageToTxn(message, rtm.consumerGroupId, nil)
	if err != nil {
		fmt.Println("Error adding message to transaction:", err)
		rtm.handleTxnError(producer, message, err, func() error {
			return producer.AddMessageToTxn(message, rtm.consumerGroupId, nil)
		})
		return nil
	}

	// Commit the transaction
	err = producer.CommitTxn()
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		rtm.handleTxnError(producer, message, err, func() error {
			return producer.CommitTxn()
		})
		return nil
	}

	fmt.Println("Message claimed:", message.Topic, message.Partition, message.Offset)
	return nil
}

func (rtm *retriableTransactionMaker) handleTxnError(
	producer sarama.AsyncProducer,
	message *sarama.ConsumerMessage,
	err error,
	defaultHandler func() error,
) {
	fmt.Printf("Message consumer: unable to process transaction: %+v", err)
	for {
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			fmt.Println("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			rtm.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			err = producer.AbortTxn()
			if err != nil {
				fmt.Printf("Message consumer: unable to abort transaction: %+v", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			rtm.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		// if not you can retry
		err = defaultHandler()
		if err == nil {
			return
		}
	}
}
