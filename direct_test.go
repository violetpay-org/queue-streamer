package qstreamer_test

import (
	"github.com/stretchr/testify/assert"
	qstreamer "github.com/violetpay-org/queue-streamer"
	"testing"
	"time"
)

var dbrokers = []string{"localhost:9093"}
var dTopic = qstreamer.Topic("test", 1)
var dTopic2 = qstreamer.Topic("test2", 2)

func TestNewDirectStreamer(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("NewDirectStreamer", func(t *testing.T) {
		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		assert.NotNil(t, ds)
	})
}

func TestDirectStreamer_Config(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("No Config set", func(t *testing.T) {
		t.Cleanup(func() {
			ds = nil
		})

		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		ok, _ := ds.Config()

		assert.False(t, ok)
	})

	t.Run("Set Config", func(t *testing.T) {
		t.Cleanup(func() {
			ds = nil
		})

		ms := qstreamer.NewPassThroughSerializer()
		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		cfg := qstreamer.NewStreamConfig(ms, dTopic)
		ds.SetConfig(cfg)

		ok, cfg := ds.Config()
		assert.True(t, ok)

		assert.Equal(t, dTopic, cfg.Topic())
		assert.Equal(t, ms, cfg.MessageSerializer())
	})

	t.Run("Set Config multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			ds = nil
		})

		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")

		ms1 := qstreamer.NewPassThroughSerializer()
		ms2 := qstreamer.NewPassThroughSerializer()

		cfg1 := qstreamer.NewStreamConfig(ms1, dTopic)
		cfg2 := qstreamer.NewStreamConfig(ms2, dTopic2)

		ds.SetConfig(cfg1)
		ds.SetConfig(cfg2)

		ok, dsCfg := ds.Config()
		assert.True(t, ok)
		assert.Equal(t, dTopic2, dsCfg.Topic())
		assert.Equal(t, ms2, dsCfg.MessageSerializer())
	})
}

func TestDirectStreamer_Consumer(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("Consumer", func(t *testing.T) {
		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		assert.NotNil(t, ds.Consumer())
	})
}

func TestDirectStreamer_Topic(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("Topic", func(t *testing.T) {
		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		assert.Equal(t, dTopic, ds.Topic())
	})
}

func TestDirectStreamer_GroupId(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("GroupID", func(t *testing.T) {
		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "testGroupId")
		assert.Equal(t, "testGroupId", ds.GroupId())
	})
}

func TestDirectStreamer_Run(t *testing.T) {
	var ds *qstreamer.DirectStreamer

	t.Cleanup(func() {
		ds = nil
	})

	t.Run("Run", func(t *testing.T) {
		t.Cleanup(func() {
			err := ds.Stop()
			assert.Nil(t, err)
			ds = nil
		})

		ds = qstreamer.NewDirectStreamer(dbrokers, dTopic, "")
		cfg := qstreamer.NewStreamConfig(qstreamer.NewPassThroughSerializer(), dTopic)
		ds.SetConfig(cfg)

		assert.NotPanics(t, func() {
			go ds.Run()
			time.Sleep(1 * time.Second)
		})
	})
}
