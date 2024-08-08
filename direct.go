package qstreamer

import (
	"github.com/violetpay-org/queue-streamer/common"
	"github.com/violetpay-org/queue-streamer/internal"
)

// DirectStreamer is a streamer that streams messages from a topic to a topic.
type DirectStreamer struct {
	ts TopicStreamer
}

// NewDirectStreamer creates a new topic streamer that streams messages from a topic to a topic.
// The streamer is configured with a list of brokers, a topic to stream from and a consumer group id .
// If you want to override the default configuration of the sarama consumer and producer, you can pass additional arguments.
//   - ds := NewDirectStreamer(brokers, topic, groupId)
//   - ds := NewDirectStreamer(brokers, topic, groupId, consumerConfig, producerConfig)
//   - ds := NewDirectStreamer(brokers, topic, groupId, nil, producerConfig)
func NewDirectStreamer(brokers []string, src common.Topic, groupId string, args ...interface{}) *DirectStreamer {
	ts := NewTopicStreamer(brokers, src, groupId, args...)
	return &DirectStreamer{
		ts: *ts,
	}
}

func (ds *DirectStreamer) Config() (bool, StreamConfig) {
	if len(ds.ts.configs) == 0 {
		return false, StreamConfig{}
	}

	return true, ds.ts.configs[0]
}

func (ds *DirectStreamer) SetConfig(config StreamConfig) {
	if len(ds.ts.configs) == 0 {
		ds.ts.configs = append(ds.ts.configs, config)
	} else {
		ds.ts.configs[0] = config
	}
}

func (ds *DirectStreamer) Topic() common.Topic {
	return ds.ts.Topic()
}

func (ds *DirectStreamer) Consumer() internal.IStreamConsumer {
	return ds.ts.Consumer()
}

func (ds *DirectStreamer) GroupId() string {
	return ds.ts.GroupId()
}

func (ds *DirectStreamer) Run() {
	ds.ts.Run()
}

func (ds *DirectStreamer) Stop() error {
	return ds.ts.Stop()
}
