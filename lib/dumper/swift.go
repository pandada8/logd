package dumper

import (
	"io"
	"log"
	"os"
	"path"

	"github.com/ncw/swift"
	"github.com/pandada8/logd/lib/common"
	"github.com/thoas/go-funk"
)

type SwiftDumper struct {
	connection    *swift.Connection
	prefix        string
	containerName string
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
	d.containerName = common.GetStringBy(config, "container", "log")
	names, err := d.connection.ContainerNames(&swift.ContainersOpts{Prefix: d.containerName})
	if err != nil {
		return
	}
	if !funk.ContainsString(names, d.containerName) {
		log.Println("[swift] container not found. creating ....")
		err = d.connection.ContainerCreate(d.containerName, nil)
	}
	d.prefix = common.GetStringBy(config, "prefix", "")
	return
}

func (d *SwiftDumper) HandleFile(file, name string) error {
	name = path.Join(d.prefix, name)
	writer, err := d.connection.ObjectCreate(d.containerName, name, false, "", "application/zstd", nil)
	defer writer.Close()
	if err != nil {
		return err
	}
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, f)
	return err
}
