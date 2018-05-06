package dumper

type Dumper interface {
	Init(config map[interface{}]interface{}) error
	WriteLine(line string) error
	Close() error
}

func GetDumper(name string, config map[interface{}]interface{}) Dumper {
	var dump Dumper
	switch name {
	case "fs":
		dump = &FSDumper{}
	case "hdfs":
		dump = &HDFSDumper{}
	case "s3":
		dump = &S3Dumper{}
	case "stdout":
		dump = &STDOUTDumper{}
	default:
		return nil
	}
	dump.Init(config)
	return dump
}
