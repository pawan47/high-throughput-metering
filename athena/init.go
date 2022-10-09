package athena

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

// AWSAthena implements SQEService.Amazon Athena is an interactive query service
// that makes it easy to analyze data in Amazon S3 using standard SQL.
// Amazon Athena uses Presto with ANSI SQL support and works with a variety of
// standard data formats, including CSV, JSON, ORC, Avro, and Parquet.
type AWSAthena struct {
	athena *athena.Athena
}

// InitAthena initializes session to AWS Athena.
func InitAthena(awscfg *aws.Config) *AWSAthena {
	sess := session.Must(session.NewSession(awscfg))
	svc := athena.New(sess)

	return &AWSAthena{svc}
}
