package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/pandada8/logd/lib/common"
	"github.com/pandada8/logd/lib/sig"
	"github.com/spf13/viper"
)

var (
	mode    = ""
	msgChan = make(chan *common.Message)
	ctlSig  *sig.Sig
	force   = false
)

func signalHandler(ch chan os.Signal) {
	for {
		s := <-ch
		switch s {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			if force {
				os.Exit(255)
			} else {
				fmt.Printf("signal %v received, prepare for quiting\n", s)
				ctlSig.Send("quit")
				force = true
			}

		}
	}
}

func usage() {
	fmt.Println("./logd <subcommand> -- SO_REUSEPORT is used, and you need a redis for message queue broker. subcommand could be 'collect' or 'dump'")
}

func getLength(i interface{}) int {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Array || v.Kind() == reflect.Slice {
		return v.Len()
	}
	return 0
}

func main() {
	validString := map[string]bool{"aio": true, "collect": true, "dump": true}
	ctlSig = sig.NewStringSig()
	log.Println("loading config...")
	viper.SetDefault("listen", "udp://localhost:1514")
	viper.SetDefault("verbose", true)
	viper.SetDefault("reuseport", true)
	viper.SetDefault("redis", "localhost:6379")
	viper.SetDefault("compress_level", 1)
	viper.SetDefault("step_size", 1e3)
	viper.SetDefault("limit", 5e3)
	viper.SetDefault("dumper_concurrency", 2)
	viper.SetConfigName("logd.cfg")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Printf("failed to load config: %s", err)
		os.Exit(2)
		return
	}
	if getLength(viper.Get("output")) == 0 {
		log.Println("no output configured")
		os.Exit(3)
		return
	}
	if len(os.Args) != 1 {
		m := os.Args[1]
		if _, ok := validString[m]; !ok {
			usage()
			os.Exit(1)
			return
		}
		mode = m
	} else {
		usage()
		os.Exit(1)
	}
	log.Printf("%s mode started", mode)

	singalChan := make(chan os.Signal, 1)
	signal.Notify(singalChan, syscall.SIGINT, syscall.SIGTERM)
	go signalHandler(singalChan)

	switch mode {
	case "collect":
		c := NewCollector()
		go c.Listen()
	case "dump":
		d := NewDumperBridge()
		go d.Start()
	}
	<-ctlSig.Close
	log.Println("quited")
}
