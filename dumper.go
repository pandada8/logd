package main

import (
	"fmt"
	"github.com/pandada8/logd/lib/dumper"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/DataDog/zstd"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

type DumperBridge struct {
	redis        *redis.Client
	redisCluster *redis.ClusterClient
	isCluster    bool
	validKeys    []string
	limit        int
	concurrency  chan int
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
		limit:        viper.GetInt("limit"),
		concurrency:  make(chan int, viper.GetInt("dumper_concurrency")),
	}
}

func (d *DumperBridge) GenerateFile(key string, force bool) (string, error) {
	d.concurrency<-1
	defer func() {
		<-d.concurrency
	}()
	var (
		dumped int
		limit  int
	)
	stepSize := viper.GetInt("step_size")
	file, err := ioutil.TempFile("", "dumper")
	defer file.Close()
	defer func() {
		if err != nil {
			os.Remove(file.Name())
		}
	}()
	if err != nil {
		return "", err
	}
	zfile := zstd.NewWriterLevel(file, viper.GetInt("compress_level"))
	defer zfile.Close()
	if force {
		limit64, err := d.redis.LLen(key).Result()
		if err != nil {
			return "", err
		}
		limit = int(limit64)
	} else {
		limit = d.limit
	}
	for dumped = 0; dumped < limit; {
		result, err := d.redis.LRange(key, int64(dumped), int64(dumped+stepSize)).Result()
		if err != nil {
			return "", err
		}
		for _, line := range result {
			zfile.Write([]byte(line + "\n"))
		}
		dumped += len(result)
	}
	err = d.redis.LTrim(key, int64(dumped), -1).Err()
	if err != nil {
		return "", err
	}
	
	return file.Name(), nil
}

func (d *DumperBridge) DumpKey(key string) (err error) {
	dumped, err := d.GenerateFile(key, force)
	if err != nil {
		return err
	}
	defer os.Remove(dumped)
	newName := fmt.Scanf("%s.%d.json.zstd", key, time.Now().Unix())
	err = dumper.GetDumper(key).HandleFile(dumped, newName)
	return
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
			if num > int64(d.limit) {
				ret = append(ret, key)
			}
		}
	} else {
		for _, key := range d.validKeys {
			num, err := d.redis.LLen(key).Result()
			if err != nil {
				log.Printf("Error when checking, given up: %s", err)
				break
			}
			if num > int64(d.limit) {
				ret = append(ret, key)
			}
		}
	}
	return ret
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
			for _, key := range shouldDump {
				dumped, err := d.DumpKey(key, ctl == "quit")
				if 
			}
		}
		if ctl == "quit" {
			return
		}
		// update the checkInterval dynamically
		time.Sleep(checkInterval)
	}
}
