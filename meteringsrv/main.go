package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"metering/zenskar-project/athena"
	"metering/zenskar-project/kinesis"
	"metering/zenskar-project/meteringsrv/depend"
	"metering/zenskar-project/meteringsrv/handlers"
)

func main() {
	ctx := context.Background()

	// init dependencies
	dep := &depend.Dependency{}
	awscfg := &aws.Config{
		Region: aws.String("<insert_correct_value>"),
		Credentials: credentials.NewStaticCredentials("<insert_correct_value>",
			"<insert_correct_value>", "<insert_correct_value>"),
	}

	dep.AWSAthena = athena.InitAthena(awscfg)
	dep.GlueDBName = "default"
	dep.AthenaOutputLoc = "s3://billingqueryoutput"
	dep.KinesisStreamName = "billingdatastream"
	dep.Kinesis = kinesis.Init(awscfg)

	// init handlers
	handler := handlers.Init(ctx, dep)

	mux := http.NewServeMux()
	// register funcs
	// returns basic information about the handlers
	mux.HandleFunc("/", handler.AboutMe)
	// billing related handler
	mux.HandleFunc("/meter", handler.Meter)

	// run server
	err := http.ListenAndServe(":3333", mux)
	// IF error is not server closed then panic
	if !errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("error starting server: %s\n", err)
		panic(err)
	}
}
