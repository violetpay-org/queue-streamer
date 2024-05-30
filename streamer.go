package qstreamer

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
)

type TopicStreamer struct {
	topic   shared.Topic
	specs   []StreamConfig
	cancels map[StreamConfig]context.CancelFunc
	conn    sarama.Client
}

func NewTopicStreamer(brokers []string, topic shared.Topic) *TopicStreamer {
	conn, err := sarama.NewClient(brokers, internal.NewSaramaConfig())
	if err != nil {
		panic(err)
	}

	return &TopicStreamer{
		topic:   topic,
		cancels: make(map[StreamConfig]context.CancelFunc),
		conn:    conn,
	}
}

func (ts *TopicStreamer) AddConfig(spec StreamConfig) {
	ts.specs = append(ts.specs, spec)
}

func (ts *TopicStreamer) Run() {
	for _, spec := range ts.specs {
		fmt.Println("Running spec")
		ts.run(ts.topic, spec.MessageSerializer(), spec.Topic(), "group")
	}
}

// run starts one goroutine for each stream spec
func (ts *TopicStreamer) run(origin shared.Topic, ms shared.MessageSerializer, destination shared.Topic, groupId string) context.CancelFunc {
	consumer := internal.NewStreamConsumer(
		ms,
		destination,
		groupId,
		ts.conn,
	)

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroupFromClient(groupId, ts.conn)
	if err != nil {
		panic(err)
	}

	fmt.Println("Running consumer", origin.Name(), destination.Name())

	go func() {
		fmt.Println("goroutine")
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{origin.Name()}, consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				fmt.Println("Error from consumer: ", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return cancel
}

func (ts *TopicStreamer) StopAll() {
	for _, cancel := range ts.cancels {
		cancel()
	}
}

func (ts *TopicStreamer) Stop(spec StreamConfig) {
	if cancel, ok := ts.cancels[spec]; ok {
		cancel()
		fmt.Println("Spec stopped")
		return
	}

	fmt.Println("Spec not found")
}

func NewTopic(name string, partition int32) shared.Topic {
	return shared.NewTopic(name, partition)
}
