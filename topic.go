package qstreamer

import "github.com/violetpay-org/queue-streamer/shared"

// Topic is a struct that represents a topic in a message broker.
// It is treated as a just "Value Object" and should not contain any business logic.
type Topic struct {
	Name      string
	Partition int32
}

func convertTopic(topic Topic) shared.Topic {
	return shared.Topic{Name: topic.Name, Partition: topic.Partition}
}
