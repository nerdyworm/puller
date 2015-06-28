package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"gopkg.in/redis.v3"

	"github.com/nerdyworm/puller"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	p := puller.New(puller.Options{
		MaxBacklogSize: 10,
		Redis:          client,
	})

	go func() {
		ticker := time.NewTicker(time.Second * 1)
		for _ = range ticker.C {
			p.Push("global", "Hello world")
		}
	}()

	// /?channelName=0&channelName2=0
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		channels := puller.Channels{}
		for key, id := range r.URL.Query() {
			lastID, _ := strconv.Atoi(id[0])
			channels[key] = int64(lastID)
		}

		backlog, err := p.Pull(channels, 10*time.Second)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		json.NewEncoder(w).Encode(backlog)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
