package common

// MessageSerializer is an interface that defines the behavior of a message serializer.
// It is used to convert a message to a message that can be produced to a message broker.
type MessageSerializer interface {
	MessageToProduceMessage(value string) string
}
