package qstreamer

import "github.com/violetpay-org/queue-streamer/common"

type StreamConfig struct {
	ms    common.MessageSerializer
	topic common.Topic
}

func NewStreamConfig(ms common.MessageSerializer, topic common.Topic) StreamConfig {
	return StreamConfig{
		ms:    ms,
		topic: topic,
	}
}

func (ss StreamConfig) MessageSerializer() common.MessageSerializer {
	return ss.ms
}

func (ss StreamConfig) Topic() common.Topic {
	return ss.topic
}
