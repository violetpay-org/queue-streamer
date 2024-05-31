package qstreamer

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/internal"
	"github.com/violetpay-org/queue-streamer/shared"
)

type TopicStreamer struct {
	topic   shared.Topic
	configs []StreamConfig
	cancels map[StreamConfig]context.CancelFunc

	// For kafka
	brokers        []string
	consumerConfig *sarama.Config
	producerConfig *sarama.Config
}

// NewTopicStreamer creates a new topic streamer that streams messages from a topic to other topics.
// The streamer is configured with a list of brokers and a topic to stream from.
// If you want to override the default configuration of the sarama consumer and producer, you can pass additional arguments.
//   - ts := NewTopicStreamer(brokers, topic)
//   - ts := NewTopicStreamer(brokers, topic, consumerConfig, producerConfig)
//   - ts := NewTopicStreamer(brokers, topic, nil, producerConfig)
func NewTopicStreamer(brokers []string, topic shared.Topic, args ...interface{}) *TopicStreamer {
	var ccfg *sarama.Config
	var pcfg *sarama.Config

	switch len(args) {
	case 1:
		panic("Invalid number of arguments")
	case 2:
		consumerConfig, ok := args[0].(*sarama.Config)
		if ok || consumerConfig != nil {
			ccfg = consumerConfig
		}

		producerConfig, ok := args[1].(*sarama.Config)
		if ok || producerConfig != nil {
			pcfg = producerConfig
		}
	default:
		ccfg = nil
		pcfg = nil
	}

	return &TopicStreamer{
		topic:          topic,
		cancels:        make(map[StreamConfig]context.CancelFunc),
		brokers:        brokers,
		consumerConfig: ccfg,
		producerConfig: pcfg,
	}
}

func (ts *TopicStreamer) AddConfig(spec StreamConfig) {
	ts.configs = append(ts.configs, spec)
}

func (ts *TopicStreamer) Run() {
	dests := make([]shared.Topic, 0)
	mss := make([]shared.MessageSerializer, 0)
	for _, config := range ts.configs {
		dests = append(dests, config.Topic())
		mss = append(mss, config.MessageSerializer())
	}

	ts.run(ts.topic, dests, mss, "group")
}

// run starts one goroutine for each stream spec
func (ts *TopicStreamer) run(origin shared.Topic, dests []shared.Topic, serializers []shared.MessageSerializer, groupId string) context.CancelFunc {
	if dests == nil || len(dests) == 0 {
		panic("No dests")
	}

	if serializers == nil || len(serializers) == 0 {
		panic("No message serializers")
	}

	if len(serializers) != len(dests) {
		panic("Number of message serializers must match number of dests")
	}

	consumer := internal.NewStreamConsumer(
		origin,
		dests,
		serializers,
		groupId,
		ts.brokers,
		ts.consumerConfig,
		ts.producerConfig,
	)

	ctx, cancel := context.WithCancel(context.Background())
	go consumer.StartAsGroup(ctx)

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
