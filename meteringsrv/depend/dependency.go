package depend

import (
	"metering/zenskar-project/athena"
	kinesis "metering/zenskar-project/kinesis"
)

type Dependency struct {
	AWSAthena         *athena.AWSAthena
	GlueDBName        string
	AthenaOutputLoc   string
	KinesisStreamName string
	Kinesis           kinesis.DataStream
}
