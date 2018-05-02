package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandada8/logd/lib/sig"
	"github.com/spf13/viper"

	syslog "github.com/influxdata/go-syslog/rfc5424"
	"github.com/tidwall/evio"
)

var (
	mode    = "aio"
	msgChan = make(chan *syslog.SyslogMessage)
	ctlSig  sig.Sig
)

func signalHandler(ch chan os.Signal) {
	for {
		s := <-ch
		switch s {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			fmt.Printf("signal %v received, prepare for quiting", s)
			ctlSig.Send("quit")
		}
	}
}

func usage() {
	fmt.Println("you can run this program in two mode")
	fmt.Println("./logd  --  aio mode, all logic in run in one process and SO_REUSEPORT is not used")
	fmt.Println("./logd <subcommand> -- SO_REUSEPORT is used, and you need a redis for message queue broker. subcommand could be 'collect' or 'dump'")
}

func main() {
	validString := map[string]bool{"aio": true, "collect": true, "dump": true}

	log.Println("loading config...")
	viper.SetDefault("listen", "udp://localhost:1514")
	viper.SetDefault("verbose", true)
	viper.SetDefault("reuseport", true)
	viper.SetConfigName("logd.cfg")
	viper.AddConfigPath(".")
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
	signal.Notify(singalChan, syscall.SIGTERM, syscall.SIGINT)
	go signalHandler(singalChan)

	switch mode {
	case "aio":
		// start dumper
		go func() {
			dumper()
		}()
		fallthrough
	case "collect":
		serve(viper.GetString("listen"))
	case "dump":
		// start dumper
		fmt.Println("not implemented yet")
	}
}

func serve(listen string) {
	//TODO: handle ^C
	p := syslog.NewParser()
	var events evio.Events
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

func dumper() {

	ctlChan := ctlSig.Recv()

	defer func() {
		ctlSig.Clean(ctlChan)
	}()
	for {
		select {
		case ctl := <-*ctlChan:
			if ctl == "quit" {
				return
			}
		case msg := <-msgChan:
			fmt.Println(msg)
		}
	}
}
