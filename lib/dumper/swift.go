package dumper

import (
	"log"

	"github.com/ncw/swift"
	"github.com/pandada8/logd/lib/common"
	"github.com/thoas/go-funk"
)

type SwiftDumper struct {
	connection *swift.Connection
}

func (d *SwiftDumper) Init(config map[interface{}]interface{}) (err error) {
	d.connection = &swift.Connection{
		UserName: common.GetStringBy(config, "username", ""),
		ApiKey:   common.GetStringBy(config, "apiKey", ""),
		AuthUrl:  common.GetStringBy(config, "authUrl", ""),
		Domain:   common.GetStringBy(config, "domain", ""),
	}
	err = d.connection.Authenticate()
	if err != nil {
		return
	}
	containerName := common.GetStringBy(config, "container", "log")
	names, err := d.connection.ContainerNames(&swift.ContainersOpts{Prefix: containerName})
	if err != nil {
		return
	}
	if !funk.ContainsString(names, containerName) {
		log.Println("[swift] container not found. creating ....")
		err = d.connection.ContainerCreate(containerName, nil)
	}
	return
}

func (d *SwiftDumper) WriteLine(line string) error {
	return nil
}

func (d *SwiftDumper) Close() error {
	return nil
}
