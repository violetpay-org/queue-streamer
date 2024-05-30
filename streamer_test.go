package qstreamer_test

import (
	qstreamer "github.com/violetpay-org/queue-streamer"
	"testing"
	"time"
)

type TestSerializer struct {
}

func (ts *TestSerializer) MessageToProduceMessage(value string) string {
	return value
}

func NewTestSerializer() *TestSerializer {
	return &TestSerializer{}
}

func TestTopicStreamer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	endTopic := qstreamer.NewTopic("te", 3)
	serializer := NewTestSerializer()
	spec := qstreamer.NewStreamConfig(serializer, endTopic)

	topicStreamer := qstreamer.NewTopicStreamer(brokers,
		qstreamer.NewTopic(
			"test", 3,
		),
	)
	topicStreamer.AddConfig(spec)

	topicStreamer.Run()
	defer topicStreamer.StopAll()
	time.Sleep(1200 * time.Second)
}
