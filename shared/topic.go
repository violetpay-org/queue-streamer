package shared

type Topic struct {
	topic     string
	partition int32
}

func (t Topic) Name() string {
	return t.topic
}

func (t Topic) Partition() int32 {
	return t.partition
}

func NewTopic(name string, partition int32) Topic {
	return Topic{
		topic:     name,
		partition: partition,
	}
}
