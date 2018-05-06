package dumper

import (
	"fmt"
)

type STDOUTDumper struct {
	prefix string
}

func (d *STDOUTDumper) Init(config map[interface{}]interface{}) (err error) {
	if prefix, found := config["prefix"]; found {
		d.prefix = prefix.(string)
	} else {
		d.prefix = "default"
	}
	return nil
}

func (d *STDOUTDumper) WriteLine(line string) error {
	fmt.Printf("LOG [%s] %s\n", d.prefix, line)
	return nil
}

func (d *STDOUTDumper) Close() error {
	return nil
}
