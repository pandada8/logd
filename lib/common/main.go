package common

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/globalsign/mgo/bson"
	syslog "github.com/influxdata/go-syslog/rfc5424"
)

type Message struct {
	Output  string
	Payload map[string]interface{}
}

func isZeroValue(v interface{}) bool {
	value := reflect.ValueOf(v)
	if value.Kind() == reflect.Array || value.Kind() == reflect.Slice {
		return reflect.ValueOf(v).Len() == 0
	}
	if value.Type().Name() == "bool" {
		return false
	}
	return v == reflect.Zero(reflect.TypeOf(v)).Interface()
}

func getConvertName(name string) string {
	return strings.ToLower(name[:1]) + name[1:]
}

func Marshal(input interface{}) (ret map[string]interface{}, err error) {
	indirect := reflect.ValueOf(input)
	if indirect.Kind() != reflect.Struct {
		err = errors.New("Wrong Type")
	}
	ret = bson.M{}
	for i := 0; i < indirect.NumField(); i++ {

		field := indirect.Field(i)
		fieldName := indirect.Type().Field(i).Name
		mappedFieldName := getConvertName(fieldName)

		if isZeroValue(field.Interface()) {
			continue
		}

		if field.Kind() == reflect.Ptr {
			field = field.Elem()
		}
		ret[mappedFieldName] = field.Interface()
	}
	return
}

func NewMessage(m *syslog.SyslogMessage) *Message {
	payload, _ := Marshal(m)
	return &Message{
		Payload: payload,
	}
}

func (m *Message) ToJSONString() string {
	var (
		payload []byte
		err     error
	)
	payload, err = json.Marshal(m.Payload)
	if err != nil {
		payload, _ = json.Marshal(map[string]string{"__err": err.Error()})
	}
	return string(payload)
}

func UnitParser(string) int {
	return 0
}

func GetStringBy(source interface{}, path string) string {
	s, can := GetBy(source, path).(string)
	if !can {
		return ""
	} else {
		return s
	}
}

func GetBy(source interface{}, path string) interface{} {
	var t interface{} = source
	for _, name := range strings.Split(path, ".") {
		switch tt := t.(type) {
		case map[interface{}]interface{}:
			t = tt[name]
		case map[string]interface{}:
			t = tt[name]
		default:
			t = tt
			break
		}
		if t == nil {
			break
		}
	}
	return t
}

func ToStringMap(source map[interface{}]interface{}) (output map[string]string) {
	for k, v := range source {
		k, k_ok := k.(string)
		v, v_ok := v.(string)
		if k_ok && v_ok {
			output[k] = v
		}
	}
	return
}
