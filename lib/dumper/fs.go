package dumper

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/pandada8/logd/lib/common"
)

type LocalFSDumper struct {
	file           *os.File
	buf            *bufio.Writer
	location       string
	chunkSize      int
	currentSize    int
	currentChunkId int
	tmpbuf         *bytes.Buffer
}

func (d *LocalFSDumper) Init(config map[interface{}]interface{}) error {
	if location, found := config["location"]; found {
		d.location = location.(string)
	}
	if chunk, found := config["chunkSize"]; found {
		d.chunkSize = common.UnitParser(chunk.(string))
	} else {
		d.chunkSize = 30 * 1024 * 1024
	}
	files, err := ioutil.ReadDir(d.location)
	if err != nil {
		return err
	}
	// find the biggest chunkid
	for _, f := range files {
		id := strings.Split(f.Name(), ".")[0]
		n, err := strconv.Atoi(id)
		if err != nil {
			continue
		}
		if n > d.currentChunkId {
			d.currentChunkId = n
		}
	}
	d.currentChunkId += 1
	return nil
}

func (d *LocalFSDumper) prepareFile() (err error) {
	d.file, err = os.Create(path.Join(d.location))
	if err != nil {
		return err
	}
	d.buf = bufio.NewWriter(d.file)
	return
}

func (d *LocalFSDumper) WriteLine(line string) (err error) {
	if d.buf == nil {
		// init the buffer
		err = d.prepareFile()
		if err != nil {
			return
		}
	}
	var size int
	size, err = d.buf.WriteString(line)
	d.currentSize += size
	if d.currentSize >= d.chunkSize {
		// flush buffer and splitChunk
		go func(f *os.File, buf *bufio.Writer) {
			buf.Flush()
			f.Close()
		}(d.file, d.buf)
		d.currentChunkId += 1
		d.prepareFile()
	}
	return
}

func (d *LocalFSDumper) Close() error {

	if d.file != nil {
		if d.buf != nil {
			d.buf.Flush()
		}
		d.file.Close()
		d.file = nil
	}
	return nil
}
