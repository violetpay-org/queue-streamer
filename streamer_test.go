package qstreamer_test

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	qstreamer "github.com/violetpay-org/queue-streamer"
	"github.com/violetpay-org/queue-streamer/common"
)

const NUM_BROKERS = 3
const NUM_TOPICS = 4
const ENV_DIR = "./.env"

func TestTopicStreamer_NewTopicStreamer(t *testing.T) {
	err := godotenv.Load(ENV_DIR)
	assert.Nil(t, err)

	brokers, err := common.GetBrokers(NUM_BROKERS)
	assert.Nil(t, err)

	topics, err := common.GetTopics(NUM_TOPICS)
	assert.Nil(t, err)
	defaultTopic := qstreamer.Topic(topics[0], 1)

	var streamer *qstreamer.TopicStreamer

	t.Cleanup(func() {
		streamer = nil
	})

	t.Run("NewTopicStreamer without additional arguments", func(t *testing.T) {
		t.Cleanup(func() {
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
		assert.NotNil(t, streamer)
	})

	t.Run("NewTopicStreamer with additional arguments", func(t *testing.T) {
		t.Run("Invalid number of arguments", func(t *testing.T) {
			assert.Panics(t, func() {
				qstreamer.NewTopicStreamer(brokers, defaultTopic, "", nil)
			})

			assert.Panics(t, func() {
				qstreamer.NewTopicStreamer(brokers, defaultTopic, "", nil, nil, nil)
			})
		})

		t.Run("Consumer config only", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			config := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "", config, nil)
			assert.NotNil(t, streamer)
		})

		t.Run("Producer config only", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			config := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "", nil, config)
			assert.NotNil(t, streamer)
		})

		t.Run("Consumer and Producer config", func(t *testing.T) {
			t.Cleanup(func() {
				streamer = nil
			})

			consumerConfig := sarama.NewConfig()
			producerConfig := sarama.NewConfig()
			streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "", consumerConfig, producerConfig)
			assert.NotNil(t, streamer)
		})
	})
}

func TestTopicStreamer_Getters(t *testing.T) {
	err := godotenv.Load(ENV_DIR)
	brokers, err := common.GetBrokers(NUM_BROKERS)
	assert.Nil(t, err)

	topics, err := common.GetTopics(NUM_TOPICS)
	assert.Nil(t, err)
	defaultTopic := qstreamer.Topic(topics[0], 1)

	streamer := qstreamer.NewTopicStreamer(brokers, defaultTopic, "")

	t.Run("Topic", func(t *testing.T) {
		assert.Equal(t, defaultTopic, streamer.Topic())
	})

	t.Run("Consumer", func(t *testing.T) {
		assert.NotNil(t, streamer.Consumer())
	})

	t.Run("Configs", func(t *testing.T) {
		assert.Equal(t, 0, len(streamer.Configs()))
	})
}

func TestTopicStreamer_AddConfig(t *testing.T) {
	brokers, err := common.GetBrokers(NUM_BROKERS)
	assert.Nil(t, err)

	topics, err := common.GetTopics(NUM_TOPICS)
	assert.Nil(t, err)
	defaultTopic := qstreamer.Topic(topics[0], 1)

	streamer := qstreamer.NewTopicStreamer(brokers, defaultTopic, "")

	t.Run("AddConfig", func(t *testing.T) {
		config := qstreamer.NewStreamConfig(
			qstreamer.NewPassThroughSerializer(),
			defaultTopic,
		)

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
	err := godotenv.Load(ENV_DIR)
	brokers, err := common.GetBrokers(NUM_BROKERS)
	assert.Nil(t, err)

	topics, err := common.GetTopics(NUM_TOPICS)
	assert.Nil(t, err)
	defaultTopic := qstreamer.Topic(topics[0], 1)

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

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), defaultTopic)
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
		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no messageSerializer", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
		config := qstreamer.NewStreamConfig(nil, defaultTopic)
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})

	t.Run("Run with no topic", func(t *testing.T) {
		t.Cleanup(func() {
			err := streamer.Stop()
			assert.NotNil(t, err)
			streamer = nil
		})

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
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

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
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

		streamer = qstreamer.NewTopicStreamer(brokers, defaultTopic, "")
		config := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), common.Topic{Partition: 1})
		streamer.AddConfig(config)

		assert.Panics(t, streamer.Run)
	})
}

/*
큐 스트리머 이름과 파티션 수가 제대로 설정되었는지 확인합니다.
*/
func TestTopic(t *testing.T) {
	err := godotenv.Load(ENV_DIR)
	NUM_PARTITIONS := int32(1)
	topics, err := common.GetTopics(NUM_TOPICS)
	assert.Nil(t, err)
	defaultTopic := qstreamer.Topic(topics[0], NUM_PARTITIONS)

	assert.Equal(t, topics[0], defaultTopic.Name)
	assert.Equal(t, NUM_PARTITIONS, defaultTopic.Partition)
}
