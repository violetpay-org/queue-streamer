# Queue Streamer

Queue Streamer is a Go package that processes and transfers data between Kafka topics with exactly-once delivery guarantees. This package receives messages from Kafka brokers and transfers them to specified topics. This document explains how to install and use Queue Streamer.

## Installation

To install Queue Streamer, use the Go modules:

```shell
go get github.com/violetpay-org/queue-streamer
```

## Usage

Here is an example code to use Queue Streamer.

### Example

```go
package main

import (
	"github.com/violetpay-org/queue-streamer"
	"sync"
)


func main() {
	wg := &sync.WaitGroup{}
	
	brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	origin := qstreamer.NewTopic("origin-topic", 3)           // Topic name and partition

	// Serializer that converts the message to the message to be produced.
	// In this case, the message is not converted, so it is a pass-through serializer.
	serializer := qstreamer.NewPassThroughSerializer()
	
	destination1 := qstreamer.NewTopic("destination-topic-1", 5) // Topic name and partition
	destination2 := qstreamer.NewTopic("destination-topic-2", 3)
	
	streamer := qstreamer.NewTopicStreamer(brokers, origin)

	cfg := qstreamer.NewStreamConfig(serializer, destination1)
	streamer.AddConfig(cfg)

	cfg = qstreamer.NewStreamConfig(serializer, destination2)
	streamer.AddConfig(cfg)
	
	streamer.Run() // Non-blocking
	defer streamer.Stop()
	wg.Add(1)

	wg.Wait()
}
```

### Explanation

1. **Set Topics**: Use the `NewTopic()` to set the start and end topics.

2. **Use PassThroughSerializer**: Create a pass-through serializer using `NewPassThroughSerializer()` which does not manufacture the message.
   * If you want to convert the message, you can create a custom serializer that implements the Serializer interface.

3. **Set StreamConfig**: Use the `NewStreamConfig()` to configure the stream settings.

4. **Create and Configure TopicStreamer**: Use the `NewTopicStreamer()` to create the topic streamer and the `AddConfig()` method to add the stream configuration.

5. **Run and Stop Streamer**: Call the `Run()` method to start the streamer and the `Stop()` method to stop the streamer.

## Contribution

Contributions are welcome! You can contribute to the project by reporting bugs, requesting features, and submitting pull requests. 

## License

Queue Streamer is distributed under the MIT License. See the LICENSE file for more details.
