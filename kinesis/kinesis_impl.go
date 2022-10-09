package kinesis

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"

	zerror "metering/zenskar-project/error"
)

// PutRecord put records into firehose
func (k *AWSKinesis) PutRecord(ctx context.Context, record []byte,
	streamName string) *zerror.ZError {

	// validate the request
	if len(record) == 0 || streamName == "" {
		return zerror.NewZErr(zerror.INVALID_REQUEST, "invalid request for putting a request")
	}

	// Add a new line at the end of the record, as Athena only recognizes
	// JSON objects delimited by new line characters
	record = []byte(string(record) + "\n")

	recordInput := firehose.PutRecordInput{
		DeliveryStreamName: aws.String(streamName),
		Record:             &firehose.Record{Data: record},
	}

	// TODO these can be added in a batch we can leverage channels here.
	_, err := k.firehouse.PutRecordWithContext(ctx, &recordInput)
	if err != nil {
		return zerror.NewZErr(zerror.KINESIS_PUT_FAILED, fmt.Sprintf(
			"Kinesis put failed with error %v", err.Error()))
	}
	return nil
}
