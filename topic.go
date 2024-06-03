package qstreamer

import "github.com/violetpay-org/queue-streamer/shared"

type Topic struct {
	Name      string
	Partition int32
}

func convertTopic(topic Topic) shared.Topic {
	return shared.Topic{Name: topic.Name, Partition: topic.Partition}
}
