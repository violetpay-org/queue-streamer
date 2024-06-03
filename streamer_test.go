package qstreamer_test

//func TestTopicStreamer(t *testing.T) {
//	brokers := []string{"b-3.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-1.vpkafkacluster2.zy10lp.c3.kafka.ap-northeast-2.amazonaws.com:9092"}
//	endTopic := qstreamer.Topic{Name: "te", Partition: 3}
//	endTopic2 := qstreamer.Topic{Name: "te2", Partition: 5}
//
//	serializer := qstreamer.NewPassThroughSerializer()
//	spec := qstreamer.NewStreamConfig(serializer, endTopic)
//	spcc2 := qstreamer.NewStreamConfig(serializer, endTopic2)
//	startTopic := qstreamer.Topic{
//		Name:      "test",
//		Partition: 3,
//	}
//
//	topicStreamer := qstreamer.NewTopicStreamer(brokers, startTopic)
//
//	topicStreamer.AddConfig(spec)
//	topicStreamer.AddConfig(spcc2)
//
//	topicStreamer.Run()
//	defer topicStreamer.Stop()
//	time.Sleep(100 * time.Second)
//	topicStreamer.Stop()
//}
