package dumper

type S3Dumper struct {
}

func (d *S3Dumper) Init(config map[interface{}]interface{}) error {
	return nil
}

func (d *S3Dumper) WriteLine(line string) error {
	return nil
}

func (d *S3Dumper) NextChunk() error {
	return nil
}

func (d *S3Dumper) Close() error {
	return nil
}
