package puller

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

type Options struct {
	MaxBacklogSize int64
	Redis          *redis.Client
}

func New(options Options) *Puller {
	if options.Redis == nil {
		options.Redis = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	}

	return newPuller(options.Redis, options.MaxBacklogSize)
}

type Channels map[string]int64

type Message struct {
	ID      int64
	Channel string
	Payload interface{}
}

func (m Message) encode() string {
	encoded, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}

	return string(encoded)
}

func (m *Message) decode(s string) {
	json.Unmarshal([]byte(s), m)
}

type Backlog struct {
	Messages []Message
	Channels Channels
}

func (b Backlog) Empty() bool {
	return len(b.Messages) == 0
}

func (b Backlog) Size() int {
	return len(b.Messages)
}

type client struct {
	channels Channels
	messages chan Message
}

func (c *client) allowed(message Message) bool {
	_, ok := c.channels[message.Channel]
	return ok
}

func newClient(channels Channels) *client {
	return &client{channels, make(chan Message, 1)}
}

type Puller struct {
	clients       map[*client]bool
	lock          sync.RWMutex
	client        *redis.Client
	pubsub        *redis.PubSub
	subscriptions map[string]int
	maxBacklog    int64
}

func newPuller(r *redis.Client, max int64) *Puller {
	puller := &Puller{
		client:        r,
		clients:       map[*client]bool{},
		lock:          sync.RWMutex{},
		maxBacklog:    max,
		pubsub:        r.PubSub(),
		subscriptions: map[string]int{},
	}

	go puller.start()
	return puller
}

func (b *Puller) Pull(channels Channels, timeout time.Duration) (Backlog, error) {
	backlog := Backlog{
		Channels: channels,
		Messages: []Message{},
	}

	for channel, lastID := range channels {
		if lastID == 0 {
			continue
		}

		messages, err := b.messages(channel, lastID)
		if err != nil {
			return backlog, err
		}

		for _, message := range messages {
			if message.ID > lastID {
				backlog.Messages = append(backlog.Messages, message)
			}
		}
	}

	if backlog.Empty() {
		end := make(chan bool, 1)
		go func() {
			time.Sleep(timeout)
			end <- true
		}()

		client := newClient(channels)

		b.addClient(client)
		defer b.removeClient(client)

		select {
		case <-end:
			break
		case message := <-client.messages:
			backlog.Messages = append(backlog.Messages, message)
		}
	}

	for channel := range channels {
		backlog.Channels[channel] = b.last(channel)
	}

	return backlog, nil
}

func (b *Puller) addClient(c *client) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.clients[c] = true
	for channel, _ := range c.channels {
		b.subscribe(channel)
	}
}

func (b *Puller) removeClient(c *client) {
	b.lock.Lock()
	defer b.lock.Unlock()

	delete(b.clients, c)
	for channel, _ := range c.channels {
		b.unsubscribe(channel)
	}
}

func (s *Puller) subscribe(channel string) {
	s.pubsub.Subscribe(channel)

	if _, ok := s.subscriptions[channel]; !ok {
		s.subscriptions[channel] = 1
	} else {
		s.subscriptions[channel]++
	}
}

func (s *Puller) unsubscribe(channel string) {
	s.subscriptions[channel]--

	if s.subscriptions[channel] == 0 {
		s.pubsub.Unsubscribe(channel)
	}
}

func (s *Puller) start() {
	for {
		msgi, err := s.pubsub.ReceiveTimeout(time.Second * 30)
		if err != nil {
			if isTimeout(err) {
				continue

			}

			log.Println(err)
			return
		}

		switch msg := msgi.(type) {
		case *redis.Message:
			message := Message{}
			message.decode(msg.Payload)
			s.send(message)
		}
	}
}

func (s *Puller) send(message Message) {
	for client, _ := range s.clients {
		if client.allowed(message) {
			client.messages <- message
		}
	}
}

func (b *Puller) messages(channel string, lastID int64) ([]Message, error) {
	key := b.keyForChannel(channel)

	query := redis.ZRangeByScore{
		Max: "+inf",
		Min: fmt.Sprintf("%d", lastID+1),
	}

	cmd := b.client.ZRangeByScore(key, query)

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	results, err := cmd.Result()
	if err != nil {
		return nil, err
	}

	messages := make([]Message, len(results))
	for i, result := range results {
		messages[i].decode(result)
	}

	return messages, nil
}

func (b *Puller) Push(channel string, payload interface{}) error {
	nextID, err := b.next(channel)
	if err != nil {
		return err
	}

	message := Message{
		ID:      nextID,
		Channel: channel,
		Payload: payload,
	}
	encoded := message.encode()

	key := b.keyForChannel(channel)
	b.client.ZAdd(key, redis.Z{float64(nextID), encoded})
	b.client.Publish(channel, encoded)

	if nextID > b.maxBacklog {
		min := fmt.Sprintf("%d", 1)
		max := fmt.Sprintf("%d", nextID-b.maxBacklog)
		b.client.ZRemRangeByScore(key, min, max)
	}

	return nil
}

func (b *Puller) next(channel string) (int64, error) {
	key := b.idKeyForChannel(channel)
	cmd := b.client.Incr(key)
	if cmd.Err() != nil {
		return 0, cmd.Err()
	}

	return cmd.Val(), nil
}

func (b *Puller) last(channel string) int64 {
	key := b.idKeyForChannel(channel)
	cmd := b.client.Get(key)
	if cmd.Err() == redis.Nil {
		return 0
	}

	if cmd.Err() != nil {
		log.Println(cmd.Err())
		return 0
	}

	id, err := cmd.Int64()
	if err != nil {
		log.Println(err)
	}

	return id
}

func (b *Puller) clear(channel string) {
	b.client.Del(
		b.idKeyForChannel(channel),
		b.keyForChannel(channel),
	)
}

func (b *Puller) idKeyForChannel(channel string) string {
	return b.keyForChannel(channel) + "_id"
}

func (b *Puller) keyForChannel(channel string) string {
	return "__puller_" + channel
}

func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}
