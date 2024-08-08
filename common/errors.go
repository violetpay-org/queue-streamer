package common

import "errors"

var (
	ErrBrokerNotFound = errors.New("broker not found")
	ErrTopicNotFound  = errors.New("topic not found")
)
