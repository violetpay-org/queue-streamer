package qstreamer_test

import (
	"github.com/stretchr/testify/assert"
	qstreamer "github.com/violetpay-org/queue-streamer"
	"math/rand"
	"testing"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestPassThroughSerializer(t *testing.T) {
	serializer := qstreamer.NewPassThroughSerializer()
	assert.NotNil(t, serializer)

	for i := 0; i < 100; i++ {
		randomString := randStringRunes(100)
		assert.Equal(t, randomString, serializer.MessageToProduceMessage(randomString))
	}
}
