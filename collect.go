package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
	syslog "github.com/influxdata/go-syslog/rfc5424"
	"github.com/pandada8/logd/lib/common"
	"github.com/spf13/viper"
	"github.com/tidwall/evio"
)

type Collector struct {
	redis        *redis.Client
	redisCluster *redis.ClusterClient
	queueMu      sync.RWMutex
	queue        []*common.Message
	isCluster    bool
	listen       string
}

func (c *Collector) SendToRedis(msg *common.Message) {
	var err error
	if c.isCluster {
		err = c.redisCluster.RPush(msg.Output, msg.ToJSONString()).Err()
	} else {
		err = c.redis.RPush(msg.Output, msg.ToJSONString()).Err()
	}
	if err != nil {
		c.queueMu.Lock()
		c.queue = append(c.queue, msg)
		c.queueMu.Unlock()
	}

}

func (c *Collector) CleanQueue() {
	c.queueMu.RLock()
	shoudClean := len(c.queue) != 0
	c.queueMu.RUnlock()
	if shoudClean {
		c.queueMu.Lock()
		bqueue := c.queue[:]
		c.queue = []*common.Message{}
		c.queueMu.Unlock()
		for _, msg := range bqueue {
			c.SendToRedis(msg)
		}
	}
}

func NewCollector() *Collector {
	var (
		isCluster          bool
		redisClient        *redis.Client
		redisClusterClient *redis.ClusterClient
	)
	switch viper.Get("redis").(type) {
	case string:
		isCluster = false
		redisClient = redis.NewClient(&redis.Options{
			Addr:       viper.GetString("redis"),
			Password:   "",
			DB:         0,
			MaxRetries: 2,
		})
		err := redisClient.Ping().Err()
		if err != nil {
			log.Println("failed to ping redis")
		}
	case []interface{}:
		isCluster = true
		redisClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:      viper.GetStringSlice("redis"),
			Password:   "",
			MaxRetries: 2,
		})
		err := redisClusterClient.Ping().Err()
		if err != nil {
			log.Println("failed to ping redis cluster")
		}
	}
	listen := viper.GetString("listen")
	if viper.GetBool("reuseport") {
		listen = fmt.Sprintf("udp://%s?reuseport=true", listen)
	} else {
		listen = fmt.Sprintf("udp://%s", listen)
	}

	col := &Collector{
		isCluster:    isCluster,
		redis:        redisClient,
		redisCluster: redisClusterClient,
		listen:       listen,
	}

	return col
}

func (c *Collector) Listen() {
	p := syslog.NewParser()
	var events evio.Events

	ctlChan := ctlSig.Recv()
	var ctl string
	defer func() {
		ctlSig.Clean(ctlChan)
	}()

	go func() {
		for {
			ctl = <-*ctlChan
		}
	}()
	matcher := NewMatcher()
	events.Tick = func() (delay time.Duration, action evio.Action) {
		delay = 1 * time.Second
		go c.CleanQueue()
		if ctl == "quit" {
			action = evio.Shutdown
			log.Println("close Collector")
		}
		return
	}
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		// id has no means when used in udp
		var matched bool
		bestEffort := true
		m, e := p.Parse(in, &bestEffort)
		if e != nil {
			fmt.Printf("failed to parse: %s\n err: %s\n", in, e)
			// ignore
			return
		}

		msg := common.NewMessage(m)
		msg.Output, matched = matcher.Match(msg.Payload)
		if matched {
			// put into a local buffer
			go c.SendToRedis(msg)
		}

		return
	}
	log.Printf("listen at %s", c.listen)
	if err := evio.Serve(events, c.listen); err != nil {
		panic(err.Error())
		//FIXME: quit peacefully
	}
}
