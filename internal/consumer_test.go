package internal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/common"
	"github.com/violetpay-org/queue-streamer/internal"
)

var cbrokers = []string{"localhost:9093"}

// TestSerializer is a mock implementation of common.MessageSerializer
type TestSerializer struct {
}

func (ts *TestSerializer) MessageToProduceMessage(value string) string {
	return value
}

func TestStreamConsumer_AddDestination(t *testing.T) {
	origin := common.Topic{Name: "test", Partition: 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	t.Run("AddDestination", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		})

		consumer.AddDestination(common.Topic{Name: "test2", Partition: 3}, &TestSerializer{})
		assert.Equal(t, 1, len(consumer.Destinations()))
		assert.Equal(t, 1, len(consumer.MessageSerializers()))

		consumer.AddDestination(common.Topic{Name: "test3", Partition: 3}, &TestSerializer{})
		assert.Equal(t, 2, len(consumer.Destinations()))
		assert.Equal(t, 2, len(consumer.MessageSerializers()))

		assert.NotEqual(t, consumer.Destinations()[0], consumer.Destinations()[1])
	})
}

func TestStreamConsumer_Setup(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	sess := &internal.MockConsumerGroupSession{}

	t.Run("Setup", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &internal.MockConsumerGroupSession{}
		})

		err := consumer.Setup(sess)
		assert.Nil(t, err)
	})
}

func TestStreamConsumer_Cleanup(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	sess := &internal.MockConsumerGroupSession{}

	t.Run("Cleanup", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &internal.MockConsumerGroupSession{}
		})

		err := consumer.Cleanup(sess)
		assert.Nil(t, err)
	})
}

func TestStreamConsumer_ConsumeClaim(t *testing.T) {
	origin := common.Topic{Name: "test", Partition: 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	sess := &internal.MockConsumerGroupSession{}
	msg := &internal.MockConsumerGroupClaim{}

	t.Run("ConsumeClaim context canceled", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &internal.MockConsumerGroupSession{}
			msg = &internal.MockConsumerGroupClaim{}
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		})

		ctx, cancel := context.WithCancel(context.Background())
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)
		defer close(msg.DataChan)

		go func() {
			time.Sleep(1 * time.Second)
			cancel()
		}()

		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
	})

	t.Run("ConsumeClaim message channel is closed", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &internal.MockConsumerGroupSession{}
			msg = &internal.MockConsumerGroupClaim{}
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)

		go func() {
			time.Sleep(1 * time.Second)
			close(msg.DataChan)
		}()

		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
	})

	t.Run("ConsumeClaim no problems", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &internal.MockConsumerGroupSession{}
			msg = &internal.MockConsumerGroupClaim{}
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)

		go func() {
			defer close(msg.DataChan)
			time.Sleep(1 * time.Second)
			msg.DataChan <- &sarama.ConsumerMessage{
				Topic:     "test",
				Partition: 1,
				Key:       []byte("key"),
				Value:     []byte("value"),
				Offset:    0,
			}
			time.Sleep(1 * time.Second)
		}()

		assert.Equal(t, 0, len(consumer.ProducerPool().Producers()))
		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(consumer.ProducerPool().Producers()))
	})
}

func TestStreamConsumer_StartAsGroup(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	handler := &internal.MockConsumerGroupHandler{}

	t.Run("StartAsGroup context canceled", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
			handler = &internal.MockConsumerGroupHandler{}
		})

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(1 * time.Second)
			cancel()
		}()
		consumer.StartAsGroup(ctx, handler)
		assert.Equal(t, 1, handler.SetupCalled)
		assert.Equal(t, 1, handler.CleanupCalled)
	})

	t.Run("StartAsGroup panic because of empty consumer", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
			handler = &internal.MockConsumerGroupHandler{}
		})

		consumer = &internal.StreamConsumer{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		assert.Panics(t, func() {
			consumer.StartAsGroup(ctx, handler)
		})
	})

	t.Run("StartAsGroup Consume with error", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
			handler = &internal.MockConsumerGroupHandler{}
		})

		consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(1 * time.Second)
			consumer.CloseGroup()
		}()
		consumer.StartAsGroup(ctx, handler)
	})

	//t.Run("StartAsGroup unknown error", func(t *testing.T) {
	//	t.Cleanup(func() {
	//		consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
	//		handler = &internal.MockConsumerGroupHandler{}
	//	})
	//
	//	ctx, cancel := context.WithCancel(context.Background())
	//	cancel()
	//
	//	consumer.StartAsGroup(ctx, handler)
	//})
}

func TestStreamConsumer_StartAsGroupSelf(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	t.Run("StartAsGroupSelf context canceled", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
		})

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(1 * time.Second)
			cancel()
		}()
		consumer.StartAsGroupSelf(ctx)
	})
}

func TestStreamConsumer_Transaction(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	producer := &internal.MockAsyncProducer{}
	session := &internal.MockConsumerGroupSession{}

	t.Run("Transaction", func(t *testing.T) {
		consumerMsgKey := "key"
		consumerMsgValue := "value"
		msg := &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Key:       []byte(consumerMsgKey),
			Value:     []byte(consumerMsgValue),
			Offset:    0,
		}

		t.Run("No destinations", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 1, producer.AddMessageToTxnCalled)
			assert.Equal(t, 1, producer.CommitTxnCalled)

			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})

		t.Run("With destinations", func(t *testing.T) {
			t.Run("With Single destination", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
					consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
					assert.Equal(t, 0, len(consumer.Destinations()))
				})

				consumer.AddDestination(common.Topic{"test2", 3}, &TestSerializer{})
				assert.Equal(t, 1, len(consumer.Destinations()))

				producer.InputChan = make(chan *sarama.ProducerMessage, 1)
				assert.NotPanics(t, func() {
					// If channel is closed, it will cause panic.
					consumer.Transaction(producer, msg, session)
				})

				claimed := <-producer.InputChan
				value, err := claimed.Value.Encode()
				assert.Nil(t, err)

				assert.Equal(t, "test2", claimed.Topic)
				assert.Equal(t, consumerMsgValue, string(value))

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 1, producer.CommitTxnCalled)

				assert.Equal(t, 0, session.ResetOffsetCalled)
				assert.Equal(t, 0, producer.AbortTxnCalled)
			})

			t.Run("With Multiple destinations", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
					consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
					assert.Equal(t, 0, len(consumer.Destinations()))
				})

				consumer.AddDestination(common.Topic{"test2", 3}, &TestSerializer{})
				consumer.AddDestination(common.Topic{"test3", 3}, &TestSerializer{})
				assert.Equal(t, 2, len(consumer.Destinations()))

				producer.InputChan = make(chan *sarama.ProducerMessage, 2)
				assert.NotPanics(t, func() {
					// If channel is closed, it will cause panic.
					consumer.Transaction(producer, msg, session)
				})

				claimed := <-producer.InputChan
				value, err := claimed.Value.Encode()
				assert.Nil(t, err)

				assert.Equal(t, "test2", claimed.Topic)
				assert.Equal(t, consumerMsgValue, string(value))

				claimed = <-producer.InputChan
				value, err = claimed.Value.Encode()
				assert.Nil(t, err)

				assert.Equal(t, "test3", claimed.Topic)
				assert.Equal(t, consumerMsgValue, string(value))

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 1, producer.CommitTxnCalled)

				assert.Equal(t, 0, session.ResetOffsetCalled)
				assert.Equal(t, 0, producer.AbortTxnCalled)
			})

			t.Run("Producer Input channel is closed", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
					consumer = internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)
					assert.Equal(t, 0, len(consumer.Destinations()))
				})

				consumer.AddDestination(common.Topic{"test2", 3}, &TestSerializer{})
				consumer.AddDestination(common.Topic{"test3", 3}, &TestSerializer{})
				assert.Equal(t, 2, len(consumer.Destinations()))

				producer.InputChan = make(chan *sarama.ProducerMessage, 2)
				close(producer.InputChan)
				assert.Panics(t, func() {
					// If channel is closed, it will cause panic.
					consumer.Transaction(producer, msg, session)
				})

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 0, producer.AddMessageToTxnCalled)
				assert.Equal(t, 0, producer.CommitTxnCalled)

				assert.Equal(t, 0, session.ResetOffsetCalled)
				assert.Equal(t, 0, producer.AbortTxnCalled)
			})
		})
	})

	t.Run("Transaction BeginTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = &internal.MockAsyncProducer{}
			session = &internal.MockConsumerGroupSession{}
		})

		msg := &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Key:       []byte("key"),
			Value:     []byte("value"),
			Offset:    0,
		}

		t.Run("TxnStatusFlag is ProducerTxnFlagFatalError", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.BeginTxnError = errors.New("error")
			producer.TxnStatusFlag = sarama.ProducerTxnFlagFatalError

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 0, producer.AddMessageToTxnCalled)
			assert.Equal(t, 0, producer.CommitTxnCalled)

			assert.Equal(t, 1, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})

		t.Run("TxnStatusFlag is ProducerTxnFlagAbortableError", func(t *testing.T) {
			t.Run("without error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.BeginTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 0, producer.AddMessageToTxnCalled)
				assert.Equal(t, 0, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 1, producer.AbortTxnCalled)
			})

			t.Run("AbortTxn with error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.BeginTxnError = errors.New("error")
				producer.AbortTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 0, producer.AddMessageToTxnCalled)
				assert.Equal(t, 0, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 10, producer.AbortTxnCalled)
			})
		})

		t.Run("Unknown error", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.BeginTxnError = errors.New("error")

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 10, producer.BeginTxnCalled)
			assert.Equal(t, 0, producer.AddMessageToTxnCalled)
			assert.Equal(t, 0, producer.CommitTxnCalled)

			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})
	})

	t.Run("Transaction AddMessageToTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = &internal.MockAsyncProducer{}
			session = &internal.MockConsumerGroupSession{}
		})

		msg := &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Key:       []byte("key"),
			Value:     []byte("value"),
			Offset:    0,
		}

		t.Run("TxnStatusFlag is ProducerTxnFlagFatalError", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.AddMessageToTxnError = errors.New("error")
			producer.TxnStatusFlag = sarama.ProducerTxnFlagFatalError

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 1, producer.AddMessageToTxnCalled)
			assert.Equal(t, 0, producer.CommitTxnCalled)

			assert.Equal(t, 1, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})

		t.Run("TxnStatusFlag is ProducerTxnFlagAbortableError", func(t *testing.T) {
			t.Run("without error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.AddMessageToTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 0, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 1, producer.AbortTxnCalled)
			})

			t.Run("AbortTxn with error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.AddMessageToTxnError = errors.New("error")
				producer.AbortTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 0, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 10, producer.AbortTxnCalled)
			})
		})

		t.Run("Unknown error", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.AddMessageToTxnError = errors.New("error")

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 10, producer.AddMessageToTxnCalled)
			assert.Equal(t, 0, producer.CommitTxnCalled)

			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})
	})

	t.Run("Transaction CommitTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = &internal.MockAsyncProducer{}
			session = &internal.MockConsumerGroupSession{}
		})

		msg := &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Key:       []byte("key"),
			Value:     []byte("value"),
			Offset:    0,
		}

		t.Run("TxnStatusFlag is ProducerTxnFlagFatalError", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.CommitTxnError = errors.New("error")
			producer.TxnStatusFlag = sarama.ProducerTxnFlagFatalError

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 1, producer.AddMessageToTxnCalled)
			assert.Equal(t, 1, producer.CommitTxnCalled)

			assert.Equal(t, 1, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})

		t.Run("TxnStatusFlag is ProducerTxnFlagAbortableError", func(t *testing.T) {
			t.Run("without error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.CommitTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 1, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 1, producer.AbortTxnCalled)
			})

			t.Run("AbortTxn with error", func(t *testing.T) {
				t.Cleanup(func() {
					producer = &internal.MockAsyncProducer{}
					session = &internal.MockConsumerGroupSession{}
				})

				producer.CommitTxnError = errors.New("error")
				producer.AbortTxnError = errors.New("error")
				producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

				consumer.Transaction(producer, msg, session)

				assert.Equal(t, 1, producer.BeginTxnCalled)
				assert.Equal(t, 1, producer.AddMessageToTxnCalled)
				assert.Equal(t, 1, producer.CommitTxnCalled)

				assert.Equal(t, 1, session.ResetOffsetCalled)
				assert.Equal(t, 10, producer.AbortTxnCalled)
			})
		})

		t.Run("Unknown error", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				session = &internal.MockConsumerGroupSession{}
			})

			producer.CommitTxnError = errors.New("error")

			consumer.Transaction(producer, msg, session)

			assert.Equal(t, 1, producer.BeginTxnCalled)
			assert.Equal(t, 2, producer.AddMessageToTxnCalled)
			assert.Equal(t, 1, producer.CommitTxnCalled)

			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)
		})
	})
}

func TestStreamConsumer_HandleTxnError(t *testing.T) {
	origin := common.Topic{"test", 3}
	consumer := internal.NewStreamConsumer(origin, "groupId", cbrokers, nil, nil)

	producer := &internal.MockAsyncProducer{}
	message := &sarama.ConsumerMessage{}
	session := &internal.MockConsumerGroupSession{}

	t.Run("HandleTxnError", func(t *testing.T) {
		t.Cleanup(func() {
			producer = &internal.MockAsyncProducer{}
			message = &sarama.ConsumerMessage{}
			session = &internal.MockConsumerGroupSession{}
		})

		t.Run("Called defaulthandler function", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				message = &sarama.ConsumerMessage{}
				session = &internal.MockConsumerGroupSession{}
			})

			functionCalledCount := 0
			testFunction := func() {
				functionCalledCount++
			}

			consumer.HandleTxnError(producer, message, session, nil, func() error {
				testFunction()
				return nil
			})
			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)

			assert.Equal(t, 1, functionCalledCount)
		})

		t.Run("Called defaulthandler function several times for retry", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				message = &sarama.ConsumerMessage{}
				session = &internal.MockConsumerGroupSession{}
			})

			functionCalledCount := 0
			testFunction := func() error {
				functionCalledCount++
				if functionCalledCount == 10 {
					return nil
				}

				return errors.New("error")
			}

			consumer.HandleTxnError(producer, message, session, nil, testFunction)
			assert.Equal(t, 0, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)

			assert.Equal(t, 10, functionCalledCount)
		})

		t.Run("HandleTxnError with ProducerTxnFlagInError", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				message = &sarama.ConsumerMessage{}
				session = &internal.MockConsumerGroupSession{}
			})

			functionCalledCount := 0
			testFunction := func() {
				functionCalledCount++
			}

			producer.TxnStatusFlag = sarama.ProducerTxnFlagFatalError

			consumer.HandleTxnError(producer, message, session, nil, func() error {
				testFunction()
				return nil
			})
			assert.Equal(t, 1, session.ResetOffsetCalled)
			assert.Equal(t, 0, producer.AbortTxnCalled)

			assert.Equal(t, 0, functionCalledCount)
		})

		t.Run("HandleTxnError with ProducerTxnFlagAbortableError", func(t *testing.T) {
			t.Cleanup(func() {
				producer = &internal.MockAsyncProducer{}
				message = &sarama.ConsumerMessage{}
				session = &internal.MockConsumerGroupSession{}
			})

			functionCalledCount := 0
			testFunction := func() {
				functionCalledCount++
			}

			producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError

			consumer.HandleTxnError(producer, message, session, nil, func() error {
				testFunction()
				return nil
			})
			assert.Equal(t, 1, session.ResetOffsetCalled)
			assert.Equal(t, 1, producer.AbortTxnCalled)

			assert.Equal(t, 0, functionCalledCount)
		})
	})
}
