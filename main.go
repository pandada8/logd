package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"syscall"
	"time"

	"github.com/pandada8/logd/lib/common"
	"github.com/pandada8/logd/lib/dumper"
	"github.com/pandada8/logd/lib/sig"
	"github.com/spf13/viper"

	syslog "github.com/influxdata/go-syslog/rfc5424"
	"github.com/tidwall/evio"
)

var (
	mode    = "aio"
	msgChan = make(chan *common.Message)
	ctlSig  *sig.Sig
)

type Matcher struct {
	rules []MatcherRuleSet
}

type MatcherRuleSet struct {
	Rules  []MatcherRule
	Output string
}

type MatcherRule struct {
	Field string
	Match *regexp.Regexp
}

func NewMatcher() *Matcher {
	var err error
	rawRules := viper.Get("rules").([]interface{})

	if len(rawRules) == 0 {
		log.Println("warning: no rules specified, using default output")
		return &Matcher{}
	}
	rules := []MatcherRuleSet{}
	for n := len(rawRules) - 1; n >= 0; n-- {
		s := MatcherRuleSet{}
		r := rawRules[n].(map[interface{}]interface{})
		s.Output = common.GetStringBy(r, "output")
		matches, ok := common.GetBy(r, "match").(map[interface{}]interface{})
		if ok {
			for field, reg := range common.ToStringMap(matches) {
				m := MatcherRule{Field: field}
				if len(reg) == 0 {
					continue
				}
				if reg[0] == '/' && reg[len(reg)-1] == '/' {
					reg = reg[1 : len(reg)-1]
				} else {
					reg = regexp.QuoteMeta(reg)
				}
				m.Match, err = regexp.Compile(reg)
				if err != nil {
					continue
				}

			}
			rules = append(rules, s)
		}
	}
	return &Matcher{rules}
}

func (matcher *Matcher) Match(payload map[string]interface{}) (output string, matched bool) {
	if len(matcher.rules) == 0 {
		output = "default"
		matched = true
	} else {
		for _, set := range matcher.rules {
			for _, rule := range set.Rules {
				f := common.GetStringBy(payload, rule.Field)
				if f == "" {
					continue
				}
				if rule.Match.MatchString(f) {
					matched = true
					break
				}
			}
			if matched {
				output = set.Output
			}
		}
	}
	return
}

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
		listen := viper.GetString("listen")
		if viper.GetBool("reuseport") {
			listen = fmt.Sprintf("udp://%s?reuseport", listen)
		} else {
			listen = fmt.Sprintf("udp://%s", listen)
		}
		go collecter(listen)
	case "dump":
		// start dumper
		fmt.Println("not implemented yet")
	}
	<-ctlSig.Close
	log.Println("quited")
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

	matcher := NewMatcher()

	events.Tick = func() (delay time.Duration, action evio.Action) {
		delay = 1 * time.Second
		// fmt.Printf("tick %s", ctl)
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
		// FIXME: run matcher
		if mode == "aio" {
			// send to the chan
			msg := common.NewMessage(m)
			msg.Output, matched = matcher.Match(msg.Payload)
			if matched {
				msgChan <- msg
			}
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
			fmt.Println(msg)
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
