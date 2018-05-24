package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/pandada8/logd/lib/common"
	"github.com/pandada8/logd/lib/dumper"

	"github.com/DataDog/zstd"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

type DumperBridge struct {
	redis        *redis.Client
	redisCluster *redis.ClusterClient
	isCluster    bool
	limit        int

	worker *common.Worker

	dumpers map[string]*dumper.Dumper
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
	return &DumperBridge{
		isCluster:    isCluster,
		redis:        redisClient,
		redisCluster: redisClusterClient,
		limit:        viper.GetInt("limit"),
		worker:       common.NewWorker(viper.GetInt("dumper_concurrency")),
	}
}

func (d *DumperBridge) Count(key string) (int, error) {
	var (
		num int64
		err error
	)
	if d.isCluster {
		num, err = d.redisCluster.LLen(key).Result()
	} else {
		num, err = d.redis.LLen(key).Result()
	}
	return int(num), err
}

func (d *DumperBridge) Range(key string, start int, end int) []string {
	var (
		result []string
		err    error
	)
	if d.isCluster {
		result, err = d.redisCluster.LRange(key, int64(start), int64(end)).Result()
	} else {
		result, err = d.redis.LRange(key, int64(start), int64(end)).Result()
	}
	if err != nil {
		return []string{}
	}
	return result
}

func (d *DumperBridge) GenerateFile(key string) (string, error) {
	var (
		dumped int
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

	for dumped = 0; dumped < d.limit; {
		result := d.Range(key, dumped, dumped+stepSize)
		for _, line := range result {
			zfile.Write([]byte(line + "\n"))
		}
		dumped += len(result)
	}
	if d.isCluster {
		err = d.redisCluster.LTrim(key, int64(dumped), -1).Err()
	} else {
		err = d.redis.LTrim(key, int64(dumped), -1).Err()
	}
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

func (d *DumperBridge) DumpKey(key string) (err error) {
	d.worker.Run()
	defer d.worker.Done()
	count, _ := d.Count(key)
	if count <= d.limit {
		return
	}
	for i := count / d.limit; i > 0; i-- {
		newName := fmt.Sprintf("%s.%d.json.zstd", key, time.Now().UnixNano())
		log.Printf("dumping %s", newName)
		dumped, err := d.GenerateFile(key)
		defer os.Remove(dumped)
		if err != nil {
			log.Println("???", err)
		}
		err = (*d.dumpers[key]).HandleFile(dumped, newName)
		if err != nil {
			log.Println("???", err)
		}
	}
	return
}

func (d *DumperBridge) ShouldDump() []string {
	ret := []string{}
	for key, _ := range d.dumpers {
		num, err := d.Count(key)
		if err != nil {
			log.Printf("Error when checking, given up: %s", err)
			break
		}
		if num > d.limit {
			ret = append(ret, key)
		}
	}
	return ret
}

func (d *DumperBridge) LoadDumpers() {
	d.dumpers = map[string]*dumper.Dumper{}
	output := viper.Get("output").([]interface{})
	for n, i := range output {
		cfg := i.(map[interface{}]interface{})
		name := cfg["name"].(string)
		dtype := cfg["type"].(string)
		dumper := dumper.GetDumper(dtype, cfg)
		if n == 0 {
			d.dumpers["default"] = &dumper
		}
		d.dumpers[name] = &dumper
	}
}

func (d *DumperBridge) Start() {
	d.LoadDumpers()
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

	t := time.Now()
	waitTime := 1 * time.Second

	for {
		t = time.Now()
		for key, _ := range d.dumpers {
			go d.DumpKey(key)
		}
		d.worker.Wait()
		if ctl == "quit" {
			return
		}
		// update the checkInterval dynamically
		if time.Now().Sub(t) < waitTime {
			time.Sleep(waitTime - time.Now().Sub(t))
		}
	}
}
