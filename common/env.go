package common

import (
	"errors"
	"os"
	"strconv"
)

func GetBrokers(num int) ([]string, error) {
	if num < 1 {
		return []string{}, errors.New("number of brokers must be greater than 0")
	}

	brokers := make([]string, num)

	for i := 1; i <= num; i++ {
		broker := os.Getenv("KAFKA_BROKER_" + strconv.Itoa(i))
		if broker == "" {
			return []string{}, ErrBrokerNotFound
		}

		brokers[i-1] = broker
	}

	return brokers, nil
}

func GetTopics(num int) ([]string, error) {
	if num < 1 {
		return []string{}, errors.New("number of topics must be greater than 0")
	}

	topics := make([]string, num)

	for i := 1; i <= num; i++ {
		topic := os.Getenv("KAFKA_TOPIC_" + strconv.Itoa(i))
		if topic == "" {
			return []string{}, ErrTopicNotFound
		}

		topics[i-1] = topic
	}

	return topics, nil
}
