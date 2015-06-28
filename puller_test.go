package puller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var options = Options{
	MaxBacklogSize: 10,
}

func TestPullerRetrunsEmptyResponse(t *testing.T) {
	puller := New(options)
	assert.NotNil(t, puller)

	backlog, err := puller.Pull(Channels{}, 0)
	assert.Nil(t, err)
	assert.True(t, backlog.Empty())
}

func TestPullerPullsEntireBacklog(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	err := puller.Push("global", "hello")
	assert.Nil(t, err)

	backlog, err := puller.Pull(Channels{"global": -1}, 0)
	assert.Nil(t, err)
	assert.False(t, backlog.Empty())
	assert.Equal(t, 1, backlog.Size())
}

func TestRedisPullForChannelsThatHaveZero(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	err := puller.Push("global", "hello")
	assert.Nil(t, err)

	backlog, err := puller.Pull(Channels{"global": 0}, 0)
	assert.Nil(t, err)
	assert.True(t, backlog.Empty())
}

func TestRedisPullBlocksUntilNewMessage(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	go func() {
		time.Sleep(1 * time.Millisecond)
		assert.Nil(t, puller.Push("global", "hello"))
		assert.Nil(t, puller.Push("global", "world"))
	}()

	backlog, err := puller.Pull(Channels{"global": 0}, 2*time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 1, backlog.Size())

	backlog, err = puller.Pull(Channels{"global": 1}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, backlog.Size())

	backlog, err = puller.Pull(Channels{"global": -1}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, backlog.Size())
}

func TestRedisPullDoesNotReturnMessagesFromOtherChannels(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	wait := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Millisecond)
		assert.Nil(t, puller.Push("global", "hello"))
		wait <- true
	}()

	backlog, err := puller.Pull(Channels{"not-global": 0}, 2*time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 0, backlog.Size())
	assert.Equal(t, 1, len(backlog.Channels))
	assert.Equal(t, int64(0), backlog.Channels["not-global"])
	<-wait
}

func TestRedisPullUpdatesChannels(t *testing.T) {
	puller := New(options)
	puller.clear("global")
	puller.clear("private.1")

	assert.Nil(t, puller.Push("global", "hello"))
	assert.Nil(t, puller.Push("global", "world"))
	assert.Nil(t, puller.Push("private.1", "hi"))

	backlog, err := puller.Pull(Channels{"global": 0, "private.1": 0}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 0, backlog.Size())
	assert.Equal(t, int64(2), backlog.Channels["global"])
	assert.Equal(t, int64(1), backlog.Channels["private.1"])

	backlog, err = puller.Pull(Channels{"global": -1, "private.1": -1}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 3, backlog.Size())
}

func TestRedisDoesntGrowPastMaxMessagesPerChannel(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	for i := 0; i < int(options.MaxBacklogSize); i++ {
		assert.Nil(t, puller.Push("global", "hello"))
		assert.Nil(t, puller.Push("global", "world"))
	}

	backlog, err := puller.Pull(Channels{"global": -1}, 0)
	assert.Nil(t, err)
	assert.Equal(t, 10, backlog.Size())
	assert.Equal(t, int64(20), backlog.Channels["global"])
}

func TestRedisPullNotifiesMultiplePullSubscribers(t *testing.T) {
	puller := New(options)
	puller.clear("global")

	wait := make(chan bool, 2)
	go func() {
		backlog, err := puller.Pull(Channels{"global": 0}, 20*time.Millisecond)
		assert.Nil(t, err)
		assert.Equal(t, 1, backlog.Size())
		assert.Equal(t, int64(1), backlog.Channels["global"])
		wait <- true
	}()

	go func() {
		backlog, err := puller.Pull(Channels{"global": 0}, 2*time.Millisecond)
		assert.Nil(t, err)
		assert.Equal(t, 1, backlog.Size())
		assert.Equal(t, int64(1), backlog.Channels["global"])
		wait <- true
	}()

	time.Sleep(time.Millisecond)
	assert.Nil(t, puller.Push("global", "hello"))

	for i := 0; i < 2; i++ {
		<-wait
	}
}
