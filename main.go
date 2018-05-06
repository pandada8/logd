package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/pandada8/logd/lib/dumper"
	"github.com/pandada8/logd/lib/sig"
	"github.com/spf13/viper"

	syslog "github.com/influxdata/go-syslog/rfc5424"
	"github.com/tidwall/evio"
)

var (
	mode    = "aio"
	msgChan = make(chan *syslog.SyslogMessage)
	ctlSig  *sig.Sig
)

func signalHandler(ch chan os.Signal) {
	for {
		s := <-ch
		switch s {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			fmt.Printf("signal %v received, prepare for quiting\n", s)
			ctlSig.Send("quit")
		}
	}
}

func usage() {
	fmt.Println("you can run this program in two mode")
	fmt.Println("./logd  --  aio mode, all logic in run in one process and SO_REUSEPORT is not used")
	fmt.Println("./logd <subcommand> -- SO_REUSEPORT is used, and you need a redis for message queue broker. subcommand could be 'collect' or 'dump'")
}

func getLength(i interface{}) int {
	if i == nil {
		return 0
	}
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Array || v.Kind() == reflect.Slice {
		return v.Len()
	} else {
		return 0
	}
}

func main() {
	validString := map[string]bool{"aio": true, "collect": true, "dump": true}
	ctlSig = sig.NewStringSig()
	log.Println("loading config...")
	viper.SetDefault("listen", "udp://localhost:1514")
	viper.SetDefault("verbose", true)
	viper.SetDefault("reuseport", true)
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
		} else {
			mode = m
		}
	}
	log.Printf("%s mode started", mode)

	singalChan := make(chan os.Signal, 1)
	signal.Notify(singalChan, syscall.SIGINT, syscall.SIGTERM)
	go signalHandler(singalChan)

	switch mode {
	case "aio":
		// start dumper
		go aioDumper()
		fallthrough
	case "collect":
		go collecter(viper.GetString("listen"))
	case "dump":
		// start dumper
		fmt.Println("not implemented yet")
	}
	<-ctlSig.Close
	log.Println("Quited")
}

func collecter(listen string) {
	//TODO: handle ^C
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

	events.Tick = func() (delay time.Duration, action evio.Action) {
		delay = 1 * time.Second
		// fmt.Printf("tick %s", ctl)
		if ctl == "quit" {
			action = evio.Shutdown
			log.Println("Close Collector")
		}
		return
	}

	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		// id has no means when used in udp
		bestEffort := true
		m, e := p.Parse(in, &bestEffort)
		if e != nil {
			fmt.Printf("failed to parse: %s\n", in)
			// ignore
			return
		}
		if mode == "aio" {
			// send to the chan
			msgChan <- m
		} else {
			// send to the redis
			fmt.Println("")
		}
		return
	}
	log.Printf("listen at %s", listen)
	if err := evio.Serve(events, listen); err != nil {
		panic(err.Error())
		//FIXME: quit peacefully
	}
}

func aioDumper() {

	ctlChan := ctlSig.Recv()

	dumpers := map[string]dumper.Dumper{}

	output := viper.Get("output").([]interface{})
	for _, i := range output {
		cfg := i.(map[interface{}]interface{})
		name := cfg["name"].(string)
		dtype := cfg["type"].(string)
		dumper := dumper.GetDumper(dtype, cfg)
		dumpers[name] = dumper
	}

	log.Printf("loaded %d dumper(s)", len(dumpers))
	defer func() {
		ctlSig.Clean(ctlChan)
	}()
	for {
		select {
		case ctl := <-*ctlChan:
			for name, i := range dumpers {
				fmt.Printf("Closing dumper %s", name)
				i.Close()
			}
			if ctl == "quit" {
				return
			}
		case msg := <-msgChan:
			fmt.Println(msg)
			// FIXME: run the matcher
			dumper := dumpers[""]
			dumper.WriteLine(*msg.Message)
		}
	}
}
