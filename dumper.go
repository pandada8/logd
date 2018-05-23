package main

import (
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

type DumperBridge struct {
	redis        *redis.Client
	redisCluster *redis.ClusterClient
	isCluster    bool
	validKeys    []string
	limit        int64
}

func NewDumperBridge() *DumperBridge {
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
	validKeys := []string{}
	return &DumperBridge{
		isCluster:    isCluster,
		redis:        redisClient,
		redisCluster: redisClusterClient,
		validKeys:    validKeys,
		limit:        viper.GetInt64("limit"),
	}
}

func (d *DumperBridge) Dump(keys []string, force bool) {
	for _, key := range keys {
		read := c.limit
		if force {
			// try find all logs to
		} else {
			result, err := d.redis.LRange(key, 0, d.limit).Result()
			if err != nil {
				log.Println(err)
				continue
			}
			go func() {
				
			}()
		}
	}
}

func (d *DumperBridge) ShouldDump() []string {
	ret := []string{}
	if d.isCluster {
		for _, key := range d.validKeys {
			num, err := d.redisCluster.SCard(key).Result()
			if err != nil {
				log.Printf("Error when checking, given up: %s", err)
				break
			}
			if num > d.limit {
				ret = append(ret, key)
			}
		}
	} else {
		for _, key := range d.validKeys {
			num, err := d.redis.SCard(key).Result()
			if err != nil {
				log.Printf("Error when checking, given up: %s", err)
				break
			}
			if num > d.limit {
				ret = append(ret, key)
			}
		}
	}
}

func (d *DumperBridge) Start() {
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

	HandleMutex := sync.RWMutex{}
	checkInterval := 1 * time.Second

	for {
		var shouldDump []string
		if ctl != "quit" {
			HandleMutex.RLock()
			shouldDump = d.ShouldDump()
			HandleMutex.RUnlock()
		} else {
			shouldDump = d.validKeys
		}
		if len(shouldDump) > 0 {
			d.Dump(shouldDump, ctl == "quit")
		}
		if ctl == "quit" {
			return
		}
		// update the checkInterval dynamically
		time.Sleep(checkInterval)
	}
}
