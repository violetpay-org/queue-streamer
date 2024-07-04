package qstreamer

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/common"
	"github.com/violetpay-org/queue-streamer/internal"
	"sync"
)

type TopicStreamer struct {
	topic   common.Topic
	configs []StreamConfig
	cancel  context.CancelFunc
	mutex   *sync.Mutex

	consumer *internal.StreamConsumer
}

// NewTopicStreamer creates a new topic streamer that streams messages from a topic to other topics.
// The streamer is configured with a list of brokers and a topic to stream from.
// If you want to override the default configuration of the sarama consumer and producer, you can pass additional arguments.
//   - ts := NewTopicStreamer(brokers, topic)
//   - ts := NewTopicStreamer(brokers, topic, consumerConfig, producerConfig)
//   - ts := NewTopicStreamer(brokers, topic, nil, producerConfig)
func NewTopicStreamer(brokers []string, topic common.Topic, args ...interface{}) *TopicStreamer {
	var ccfg *sarama.Config
	var pcfg *sarama.Config

	switch len(args) {
	case 0: // do nothing
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
		panic("Invalid number of arguments")
	}

	consumer := internal.NewStreamConsumer(
		topic,
		"groupId",
		brokers,
		ccfg,
		pcfg,
	)

	return &TopicStreamer{
		topic:    topic,
		configs:  make([]StreamConfig, 0),
		cancel:   nil,
		consumer: consumer,
		mutex:    &sync.Mutex{},
	}
}

func (ts *TopicStreamer) Topic() common.Topic {
	return ts.topic
}

func (ts *TopicStreamer) Consumer() *internal.StreamConsumer {
	return ts.consumer
}

func (ts *TopicStreamer) Configs() []StreamConfig {
	return ts.configs
}

func (ts *TopicStreamer) AddConfig(config StreamConfig) {
	ts.configs = append(ts.configs, config)
}

func (ts *TopicStreamer) Run() {
	dests := make([]common.Topic, 0)
	mss := make([]common.MessageSerializer, 0)
	for _, config := range ts.configs {
		if config.Topic().Name == "" || config.Topic().Partition <= 0 {
			panic("Invalid topic")
		}

		if config.MessageSerializer() == nil {
			panic("No message serializer")
		}

		dests = append(dests, config.Topic())
		mss = append(mss, config.MessageSerializer())
	}

	ts.run(dests, mss)
}

func (ts *TopicStreamer) run(dests []common.Topic, serializers []common.MessageSerializer) {
	if dests == nil || len(dests) == 0 {
		panic("No dests")
	}

	for i, dest := range dests {
		ts.consumer.AddDestination(dest, serializers[i])
	}

	ctx, cancel := context.WithCancel(context.Background())
	ts.mutex.Lock()
	ts.cancel = cancel
	ts.mutex.Unlock()
	ts.consumer.StartAsGroupSelf(ctx)
}

func (ts *TopicStreamer) Stop() error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	if ts.cancel == nil {
		return errors.New("no cancel function")
	}

	ts.cancel()
	return nil
}

func Topic(name string, partition int32) common.Topic {
	return common.Topic{Name: name, Partition: partition}
}
