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
	brokers := []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}
	endTopic := qstreamer.NewTopic("te", 3)
	endTopic2 := qstreamer.NewTopic("te2", 5)
	serializer := NewTestSerializer()
	spec := qstreamer.NewStreamConfig(serializer, endTopic)
	spcc2 := qstreamer.NewStreamConfig(serializer, endTopic2)

	topicStreamer := qstreamer.NewTopicStreamer(brokers,
		qstreamer.NewTopic(
			"test", 3,
		),
	)
	topicStreamer.AddConfig(spec)
	topicStreamer.AddConfig(spcc2)

	topicStreamer.Run()
	defer topicStreamer.StopAll()
	time.Sleep(1000 * time.Second)
	topicStreamer.StopAll()
}
