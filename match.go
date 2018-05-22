package main

import (
	"log"
	"regexp"

	"github.com/pandada8/logd/lib/common"
	"github.com/spf13/viper"
)

type Matcher struct {
	Rules []MatcherRuleSet
}

type MatcherRuleSet struct {
	Rules  []MatcherRule
	Output string
}

type MatcherRule struct {
	Field  string
	Match  *regexp.Regexp
	String *string
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
		s.Output = common.GetStringBy(r, "output", "default")
		matches, ok := common.GetBy(r, "match").(map[interface{}]interface{})
		if ok {
			for field, reg := range common.ToStringMap(matches) {
				m := MatcherRule{Field: field}
				if len(reg) == 0 {
					continue
				}
				if reg[0] == '/' && reg[len(reg)-1] == '/' {
					reg = reg[1 : len(reg)-1]
					m.Match, err = regexp.Compile(reg)
				} else {
					m.String = &reg
				}
				if err != nil {
					continue
				}
				s.Rules = append(s.Rules, m)
			}
			rules = append(rules, s)
		}
	}
	return &Matcher{rules}
}

func (matcher *Matcher) Match(payload map[string]interface{}) (output string, matched bool) {
	if len(matcher.Rules) == 0 {
		output = "default"
		matched = true
	} else {
		for _, set := range matcher.Rules {
			for _, rule := range set.Rules {
				f := common.GetStringBy(payload, rule.Field, "")
				if f == "" {
					continue
				}
				if rule.Match != nil {
					if rule.Match.MatchString(f) {
						matched = true
						break
					}
				} else if rule.String != nil {
					if *rule.String == f {
						matched = true
						break
					}
				}
			}
			if matched {
				output = set.Output
				break
			}
		}
	}
	return
}
