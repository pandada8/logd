package dumper

import (
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pandada8/logd/lib/common"
)

type S3Dumper struct {
	key      string
	endpoint string
	secret   string
	bucket   string
	region   string
	prefix   string
}

func (d *S3Dumper) Init(config map[interface{}]interface{}) error {
	d.key = common.GetStringBy(config, "key", "")
	d.endpoint = common.GetStringBy(config, "endpoint", "")
	d.secret = common.GetStringBy(config, "secret", "")
	d.region = common.GetStringBy(config, "region", "")
	d.bucket = common.GetStringBy(config, "bucket", "")
	d.prefix = common.GetStringBy(config, "prefix", "")
	return nil
}

func (d *S3Dumper) HandleFile(file, name string) (err error) {
	conf := aws.Config{Region: aws.String(d.region)}
	sess := session.New(&conf)
	svc := s3manager.NewUploader(sess)

	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return err
	}

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(path.Join(d.prefix, name)),
		Body:   f,
	})
	log.Printf("uploaded chunk to %s", result.Location)
	return err
}
