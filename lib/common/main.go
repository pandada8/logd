package common

import (
	"encoding/json"

	syslog "github.com/influxdata/go-syslog/rfc5424"
)

type Message struct {
	Output  string
	Message *syslog.SyslogMessage
}

func (m *Message) ToJSONString() string {
	var (
		payload []byte
		err     error
	)
	payload, err = json.Marshal(m.Message)
	if err != nil {
		payload, _ = json.Marshal(map[string]string{"__err": err.Error()})
	}
	return string(payload)
}

func UnitParser(string) int {
	return 0
}
