package qstreamer_test

import (
	qstreamer "github.com/violetpay-org/queue-streamer"
	"testing"
	"time"
)

func TestTopicStreamer(t *testing.T) {
	brokers := []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}
	endTopic := qstreamer.Topic("te1", 3)
	endTopic2 := qstreamer.Topic("te2", 3)

	serializer := qstreamer.NewPassThroughSerializer()
	spec := qstreamer.NewStreamConfig(serializer, endTopic)
	spcc2 := qstreamer.NewStreamConfig(serializer, endTopic2)
	startTopic := qstreamer.Topic(
		"test",
		3,
	)

	topicStreamer := qstreamer.NewTopicStreamer(brokers, startTopic)

	topicStreamer.AddConfig(spec)
	topicStreamer.AddConfig(spcc2)

	topicStreamer.Run()
	defer topicStreamer.Stop()
	time.Sleep(100 * time.Second)
	topicStreamer.Stop()
}
