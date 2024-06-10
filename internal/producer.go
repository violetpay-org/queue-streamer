package internal

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/common"
	"sync"
)

// ProducerPool is a pool of producers that can be used to produce messages to Kafka for one set of brokers.
// It is not related to Transaction, Transactional Producer implements by configProvider.
type ProducerPool struct {
	locker    sync.Mutex
	producers map[common.Topic][]sarama.AsyncProducer

	// For kafka
	brokers        []string
	configProvider func() *sarama.Config
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
	producer, err := sarama.NewAsyncProducer(p.brokers, p.configProvider())
	if err != nil {
		fmt.Println("Error creating producer", err)
		return nil
	}

	return producer
}
