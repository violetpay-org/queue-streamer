package shared

type MessageSerializer interface {
	MessageToProduceMessage(value string) string
}
