package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/pandada8/logd/lib/common"
	"github.com/pandada8/logd/lib/dumper"
	"github.com/pandada8/logd/lib/sig"
	"github.com/spf13/viper"
)

var (
	mode    = ""
	msgChan = make(chan *common.Message)
	ctlSig  *sig.Sig
	force   = false
)

func GenDumpers() (dumpers map[string]dumper.Dumper) {
	dumpers = map[string]dumper.Dumper{}
	output := viper.Get("output").([]interface{})
	for n, i := range output {
		cfg := i.(map[interface{}]interface{})
		name := cfg["name"].(string)
		dtype := cfg["type"].(string)
		dumper := dumper.GetDumper(dtype, cfg)
		if n == 0 {
			dumpers["default"] = dumper
		}
		dumpers[name] = dumper
	}
	return dumpers
}

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
		go collecter()
	case "dump":
		// start dumper
		fmt.Println("not implemented yet")
	}
	<-ctlSig.Close
	log.Println("quited")
}

func collecter() {
	c := NewCollector()
	c.Listen()
}

func aioDumper() {

	ctlChan := ctlSig.Recv()

	dumpers := GenDumpers()

	log.Printf("loaded %d dumper(s)", len(dumpers))
	defer func() {
		ctlSig.Clean(ctlChan)
	}()
	for {
		select {
		case ctl := <-*ctlChan:
			for name, i := range dumpers {
				log.Printf("closing dumper %s\n", name)
				i.Close()
			}
			if ctl == "quit" {
				log.Println("closed dumper")
				return
			}
		case msg := <-msgChan:
			// FIXME: run the matcher
			dumper, found := dumpers[msg.Output]
			if found {
				dumper.WriteLine(msg.ToJSONString())
			} else {
				log.Printf("[dump] fail to find output of %s", msg.Output)
			}
		}
	}
}
