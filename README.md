# Queue Streamer
![Coverage](https://img.shields.io/badge/Coverage-100.0%25-brightgreen)
[![Test](https://github.com/violetpay-org/queue-streamer/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/violetpay-org/queue-streamer/actions/workflows/test.yml)
[![CodeQL](https://github.com/violetpay-org/queue-streamer/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/violetpay-org/queue-streamer/actions/workflows/github-code-scanning/codeql)


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
	
	brokers := []string{"localhost:9092"} 
	
	// Topic name and partition
	origin := qstreamer.Topic("origin-topic", 3)
	
	// Create a topic streamer from the brokers and the origin topic.
	streamer := qstreamer.NewTopicStreamer(brokers, origin)
	
	// Serializer that converts the message to the message to be produced. 
	// In this case, the message is not converted, so it is a pass-through serializer.
	serializer := qstreamer.NewPassThroughSerializer()
	
	// Destination topic and partition
	destination1 := qstreamer.Topic("destination-topic-1", 5)
	
	cfg := qstreamer.NewStreamConfig(serializer, destination1)
	streamer.AddConfig(cfg)

	
	go streamer.Run()
	defer streamer.Stop()
	wg.Add(1)
	
	wg.Wait()
}
```

### Explanation

1. **Set Topics**: Use the `Topic()` to set the start and end topics.

2. **Create Streamer**: Create a new streamer with the `NewTopicStreamer()` function. This function takes the Kafka brokers and the origin topic as arguments.

3. **Set Serializer**: Create a new serializer with the `NewPassThroughSerializer()` function. This function is used to convert the message to the message to be produced. In this case, the message is not converted, so it is a pass-through serializer.

4. **Set Destination Topic**: Use the `Topic()` to set the destination topic and partition.

5. **Set Configuration**: Create a new configuration with the `NewStreamConfig()` function. This function takes the serializer and the destination topic as arguments. Add the configuration to the streamer with the `AddConfig()` function.

6. **Run Streamer**: Run the streamer with the `Run()` function. This function starts the streamer and processes the messages.

## Contribution

Contributions are welcome! You can contribute to the project by reporting bugs, requesting features, and submitting pull requests. 

## License

Queue Streamer is distributed under the MIT License. See the LICENSE file for more details.
