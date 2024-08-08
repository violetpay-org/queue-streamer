package internal

import (
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/common"
	"sync"
	"time"
)

// ProducerPool is a pool of producers that can be used to produce messages to Kafka for one set of brokers.
// It is not related to Transaction, Transactional Producer implements by configProvider.
//
// Will be overridden Returns.Errors to true, because of monitoring errors.
type ProducerPool struct {
	locker    sync.Mutex
	producers map[common.Topic][]sarama.AsyncProducer

	// For kafka
	brokers        []string
	configProvider func() *sarama.Config
	monitoring     bool
}

// Take returns a producer for a given topic. If the producer does not exist, it creates a new one.
func (p *ProducerPool) Take(topic common.Topic) (producer sarama.AsyncProducer) {
	p.locker.Lock()
	defer p.locker.Unlock()

	if producers, ok := p.producers[topic]; !ok || len(producers) == 0 {
		// If there are no producers for the topic, create a new one
		producer = p.generateProducer()
		return
	}

	producer = p.producers[topic][0]
	p.producers[topic] = p.producers[topic][1:]
	return
}

// Return returns a producer to the pool.
func (p *ProducerPool) Return(producer sarama.AsyncProducer, topic common.Topic) {
	p.locker.Lock()
	defer p.locker.Unlock()

	// If the producer is closed, do not return it to the pool
	if producer == nil {
		return
	}

	// If the producer has an txError, do not return it to the pool
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		_ = producer.Close()
		return
	}

	p.producers[topic] = append(p.producers[topic], producer)
}

func (p *ProducerPool) Producers() map[common.Topic][]sarama.AsyncProducer {
	return p.producers
}

func (p *ProducerPool) Close() {
	p.locker.Lock()
	defer p.locker.Unlock()

	for _, producers := range p.producers {
		for _, producer := range producers {
			_ = producer.Close()
		}
	}
}

func NewProducerPool(brokers []string, configProvider func() *sarama.Config) *ProducerPool {
	if configProvider() == nil {
		panic("configProvider is nil")
	}

	pool := &ProducerPool{
		locker:         sync.Mutex{},
		producers:      make(map[common.Topic][]sarama.AsyncProducer),
		brokers:        brokers,
		configProvider: configProvider,
	}

	return pool
}

func (p *ProducerPool) generateProducer() sarama.AsyncProducer {
	cfg := p.configProvider()
	cfg.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(p.brokers, cfg)
	if err != nil {
		fmt.Println("Error creating producer", err)
		return nil
	}

	go p.monitorErrors(producer)

	return producer
}

func (p *ProducerPool) isMonitoring() bool {
	return p.monitoring
}

func (p *ProducerPool) monitorErrors(producer sarama.AsyncProducer) {
	p.monitoring = true
	for err := range producer.Errors() {
		fmt.Println("ERROR! Failed to produce message:", err.Err)
		if errors.Is(err.Err, sarama.ErrShuttingDown) {
			if err.Msg == nil {
				// 프로듀서가 꺼졌는데 메시지가 남아있지 않은 경우에만 break 합니다.
				// 메세지가 남아있는 경우 재발행해야 되기 때문에 break 하면 안됩니다.
				break
			}
		}

		// 재발행.
		msg := err.Msg
		p.republishMessage(msg)
	}

	p.monitoring = false
}

// republishMessage republishes a message that failed to be produced.
func (p *ProducerPool) republishMessage(msg *sarama.ProducerMessage) {
	// Republish message
	producer := p.Take(common.Topic{Name: msg.Topic, Partition: msg.Partition})
	if producer != nil {
		time.Sleep(500 * time.Millisecond)
		producer.Input() <- msg
		p.Return(producer, common.Topic{Name: msg.Topic, Partition: msg.Partition})
	} else {
		p.republishMessage(msg)
	}
}
