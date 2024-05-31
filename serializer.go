package qstreamer

type PassThroughSerializer struct {
}

func (ts *PassThroughSerializer) MessageToProduceMessage(value string) string {
	return value
}

func NewPassThroughSerializer() *PassThroughSerializer {
	return &PassThroughSerializer{}
}
