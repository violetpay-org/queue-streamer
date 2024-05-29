package qstreamer

import "github.com/violetpay-org/queue-streamer/shared"

type StreamConfig struct {
	ms    shared.MessageSerializer
	topic shared.Topic
}

func NewStreamConfig(ms shared.MessageSerializer, topic shared.Topic) StreamConfig {
	return StreamConfig{
		ms:    ms,
		topic: topic,
	}
}

func (ss StreamConfig) MessageSerializer() shared.MessageSerializer {
	return ss.ms
}

func (ss StreamConfig) Topic() shared.Topic {
	return ss.topic
}
