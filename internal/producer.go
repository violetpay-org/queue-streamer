package internal

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queue-streamer/shared"
	"sync"
)

// producerPool is a pool of producers that can be used to produce messages to Kafka for one set of brokers.
type producerPool struct {
	locker    sync.Mutex
	producers map[shared.Topic][]sarama.AsyncProducer
	conn      sarama.Client
}

// Take returns a producer for a given topic. If the producer does not exist, it creates a new one.
func (p *producerPool) Take(topic shared.Topic) (producer sarama.AsyncProducer) {
	p.locker.Lock()
	defer p.locker.Unlock()

	fmt.Println("Take producer")

	if producers, ok := p.producers[topic]; !ok || len(producers) == 0 {
		// If there are no producers for the topic, create a new one
		fmt.Println("Creating producer")
		producer = p.generateProducer(p.conn)
		fmt.Println("Producer created")
		fmt.Println(producer)
		return
	}

	fmt.Println("Producer taken from pool")
	producer = p.producers[topic][0]
	p.producers[topic] = p.producers[topic][1:]
	return
}

// Return returns a producer to the pool.
func (p *producerPool) Return(producer sarama.AsyncProducer, topic shared.Topic) {
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

func newProducerPool(connection sarama.Client) *producerPool {
	pool := &producerPool{
		locker:    sync.Mutex{},
		producers: make(map[shared.Topic][]sarama.AsyncProducer),
		conn:      connection,
	}

	return pool
}

func (p *producerPool) generateProducer(conn sarama.Client) sarama.AsyncProducer {
	fmt.Println("22")

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, NewProducerConfig())
	if err != nil {
		fmt.Println("Error creating producer", err)
		return nil
	}

	return producer
}