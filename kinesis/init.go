package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"

	zerror "metering/zenskar-project/error"
)

type AWSKinesis struct {
	firehouse *firehose.Firehose
}

type DataStream interface {
	PutRecord(ctx context.Context, record []byte,
		streamName string) *zerror.ZError
}

func Init(awscfg *aws.Config) DataStream {
	sess := session.Must(session.NewSession(awscfg))
	firehoseObj := firehose.New(sess)
	return &AWSKinesis{
		firehouse: firehoseObj,
	}
}
