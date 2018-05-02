package main

import (
	"fmt"
	"log"

	"github.com/spf13/viper"

	syslog "github.com/influxdata/go-syslog/rfc5424"
	"github.com/tidwall/evio"
)

func main() {
	log.Println("loading config...")
	viper.SetDefault("listen", "udp://localhost:1514")
	viper.SetDefault("verbose", true)
	viper.SetDefault("reuseport", true)
	viper.SetConfigName("logd.cfg")
	viper.AddConfigPath(".")
	log.Println("Server Started")
	serve(viper.GetString("listen"))
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
			fmt.Printf("%s\n", in)
			// ignore
			return
		}
		return
	}
	log.Printf("listen at %s", listen)
	if err := evio.Serve(events, listen); err != nil {
		panic(err.Error())
		//FIXME: quit peacefully
	}
}
