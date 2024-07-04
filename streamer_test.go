package qstreamer_test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	qstreamer "github.com/violetpay-org/queue-streamer"
	"github.com/violetpay-org/queue-streamer/common"
	"testing"
	"time"
)

var brokers = []string{"localhost:9093"}
var topic = qstreamer.Topic("test", 1)

func TestTopicStreamer_NewTopicStreamer(t *testing.T) {
	var streamer *qstreamer.TopicStreamer

	t.Cleanup(func() {
		streamer = nil
	})

	t.Run("NewTopicStreamer without additional arguments", func(t *testing.T) {
		t.Cleanup(func() {
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		assert.NotNil(t, streamer)
	})

	t.Run("NewTopicStreamer with additional arguments", func(t *testing.T) {
		t.Run("Invalid number of arguments", func(t *testing.T) {
			assert.Panics(t, func() {
				qstreamer.NewTopicStreamer(brokers, topic, nil)
			})

			assert.Panics(t, func() {
				qstreamer.NewTopicStreamer(brokers, topic, nil, nil, nil)
			})
		})

		t.Run("Consumer config only", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			config := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, topic, config, nil)
			assert.NotNil(t, streamer)
		})

		t.Run("Producer config only", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			config := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, topic, nil, config)
			assert.NotNil(t, streamer)
		})

		t.Run("Consumer and Producer config", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			consumerConfig := sarama.NewConfig()
			producerConfig := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, topic, consumerConfig, producerConfig)
			assert.NotNil(t, streamer)
		})
	})
}

func TestTopicStreamer_Getters(t *testing.T) {
	streamer := qstreamer.NewTopicStreamer(brokers, topic)

	t.Run("Topic", func(t *testing.T) {
		assert.Equal(t, topic, streamer.Topic())
	})

	t.Run("Consumer", func(t *testing.T) {
		assert.NotNil(t, streamer.Consumer())
	})

	t.Run("Configs", func(t *testing.T) {
		assert.Equal(t, 0, len(streamer.Configs()))
	})
}

func TestTopicStreamer_AddConfig(t *testing.T) {
	streamer := qstreamer.NewTopicStreamer(brokers, topic)

	t.Run("AddConfig", func(t *testing.T) {
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), topic)

		streamer.AddConfig(config)
		assert.Equal(t, 1, len(streamer.Configs()))
		assert.Equal(t, config, streamer.Configs()[0])

		streamer.AddConfig(config)
		assert.Equal(t, 2, len(streamer.Configs()))

		streamer.AddConfig(config)
		assert.Equal(t, 3, len(streamer.Configs()))
	})
}

func TestTopicStreamer_Run(t *testing.T) {
	var streamer *qstreamer.TopicStreamer

	t.Cleanup(func() {
		streamer = nil
	})

	t.Run("Run", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.Nil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), topic)
		streamer.AddConfig(config)

		assert.NotPanics(t, func() {
			go streamer.Run()
			time.Sleep(1 * time.Second)
		})
	})

	t.Run("Run with no dests", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})
		streamer = qstreamer.NewTopicStreamer(brokers, topic)

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no messageSerializer", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		config := qstreamer.NewStreamConfig(nil, topic)
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no topic", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), common.Topic{})
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no topic partition", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), common.Topic{Name: "test1"})
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no topic name", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, topic)
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), common.Topic{Partition: 1})
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})
}

func TestTopic(t *testing.T) {
	topicc := qstreamer.Topic("test2", 1)
	assert.Equal(t, "test2", topicc.Name)
	assert.Equal(t, int32(1), topicc.Partition)
}
