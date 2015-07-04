package puller

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
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
	add           chan *client
	remove        chan *client
	receive       chan Message
	client        *redis.Client
	pubsub        *redis.PubSub
	subscriptions map[string]int
	maxBacklog    int64
}

func newPuller(r *redis.Client, max int64) *Puller {
	puller := &Puller{
		client:        r,
		add:           make(chan *client),
		remove:        make(chan *client),
		receive:       make(chan Message),
		clients:       map[*client]bool{},
		maxBacklog:    max,
		pubsub:        r.PubSub(),
		subscriptions: map[string]int{},
	}

	go puller.run()
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
		client := newClient(channels)

		b.add <- client
		defer func() { b.remove <- client }()

		select {
		case <-time.After(timeout):
		case message := <-client.messages:
			backlog.Messages = append(backlog.Messages, message)
		}
	}

	for channel := range channels {
		backlog.Channels[channel] = b.last(channel)
	}

	return backlog, nil
}

func (s *Puller) subscribe(channel string) {
	if _, ok := s.subscriptions[channel]; !ok {
		s.subscriptions[channel] = 0
	}

	if s.subscriptions[channel] <= 0 {
		s.pubsub.Subscribe(channel)
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

func (p *Puller) run() {
	for {
		select {
		case client := <-p.add:
			p.clients[client] = true
			for channel, _ := range client.channels {
				p.subscribe(channel)
			}
		case client := <-p.remove:
			delete(p.clients, client)
			for channel, _ := range client.channels {
				p.unsubscribe(channel)
			}
		case message := <-p.receive:
			for client := range p.clients {
				if client.allowed(message) {
					delete(p.clients, client)
					client.messages <- message
					close(client.messages)
				}
			}
		}
	}
}

func (s *Puller) start() {
	for {
		msgi, err := s.pubsub.ReceiveTimeout(time.Second * 30)
		if err != nil {
			if !isTimeout(err) {
				log.Println("ERROR", err)
			}
			continue
		}

		switch msg := msgi.(type) {
		case *redis.Message:
			message := Message{}
			message.decode(msg.Payload)
			s.receive <- message
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
	cmd := b.client.ZAdd(key, redis.Z{float64(nextID), encoded})
	if cmd.Err() != nil {
		return cmd.Err()
	}

	cmd = b.client.Publish(channel, encoded)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	if nextID > b.maxBacklog {
		min := fmt.Sprintf("%d", 1)
		max := fmt.Sprintf("%d", nextID-b.maxBacklog)
		return b.client.ZRemRangeByScore(key, min, max).Err()
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
