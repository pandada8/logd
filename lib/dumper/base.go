package dumper

import "log"

type Dumper interface {
	Init(config map[interface{}]interface{}) error
	HandleFile(path, newname string) error
}

func GetDumper(name string, config map[interface{}]interface{}) Dumper {
	var dump Dumper
	switch name {
	case "fs":
		dump = &LocalFSDumper{}
	case "s3":
		dump = &S3Dumper{}
	case "swift":
		dump = &SwiftDumper{}
	default:
		return nil
	}
	err := dump.Init(config)
	if err != nil {
		log.Println("failed to gen dumper")
		return nil
	}
	return dump
}
