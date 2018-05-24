package dumper

import (
	"errors"
	"io"
	"os"
	"path"

	"github.com/pandada8/logd/lib/common"
)

type LocalFSDumper struct {
	path string
}

func (d *LocalFSDumper) Init(config map[interface{}]interface{}) error {
	d.path = common.GetStringBy(config, "path", "")
	if d.path == "" {
		return errors.New("required config path")
	}
	return nil
}

func CopyFile(dst, src string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	// no need to check errors on read only file, we already got everything
	// we need from the filesystem, so nothing can go wrong now.
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(d, s); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func (d *LocalFSDumper) HandleFile(file, name string) (err error) {
	if _, err = os.Stat(d.path); os.IsNotExist(err) {
		os.MkdirAll(d.path, 0755)
	}
	err = CopyFile(path.Join(d.path, name), file)
	return
}
