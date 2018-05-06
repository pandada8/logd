package dumper

type HDFSDumper struct {
}

func (d *HDFSDumper) Init(config map[interface{}]interface{}) error {
	return nil
}

func (d *HDFSDumper) WriteLine(line string) error {
	return nil
}

func (d *HDFSDumper) NextChunk() error {
	return nil
}

func (d *HDFSDumper) Close() error {
	return nil
}
