package internal

import (
	"fmt"
	"github.com/IBM/sarama"
	"strconv"
	"time"
)

var (
	num int = 1
)

func NewProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	// Increment num before setting the transaction ID
	id := "sarama" + strconv.Itoa(num)
	num++

	config.Producer.Transaction.ID = id
	fmt.Println("TRANSADASD" + config.Producer.Transaction.ID)
	//
	config.Net.MaxOpenRequests = 1
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	//config.Version = sarama.V3_5_1_0
	//
	//
	//
	//config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	return config
}

func NewConsumerConfig() *sarama.Config {
	config := sarama.NewConfig()

	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	return config
}

func NewSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_1_0
	config.Net.MaxOpenRequests = 1

	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	config.Producer.Transaction.ID = "sarama"
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	return config
}
