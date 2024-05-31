package qstreamer_test

import (
	"github.com/violetpay-org/queue-streamer"
	"sync"
	"testing"
	"time"
)

var wg sync.WaitGroup = sync.WaitGroup{}

func TestTopicStreamer(t *testing.T) {
	brokers := []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}
	endTopic := qstreamer.NewTopic("te", 3)
	endTopic2 := qstreamer.NewTopic("te2", 5)
	serializer := qstreamer.NewPassThroughSerializer()
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
	defer topicStreamer.Stop()
	time.Sleep(100 * time.Second)
	topicStreamer.Stop()
}
