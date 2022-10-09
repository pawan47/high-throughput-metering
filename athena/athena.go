package athena

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/service/athena"

	zerror "metering/zenskar-project/error"
)

// ExecuteQuerySync executes athena quries in sync fashion
// It first start the query execution, then it get query status using queryID till it reaches the
// terminate state, then based on state, get query result and return it
// TODO we can cahce the response of the query with queryID so that we don't need
// run to the same query again
func (ath *AWSAthena) ExecuteQuerySync(ctx context.Context, dbName, query string, outputLoc string,
	queryTimeout time.Duration) (*athena.GetQueryResultsOutput, *zerror.ZError) {
	var (
		executionId string
		zerr        *zerror.ZError
		status      string
	)

	// add querytimeout in the context
	qCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// start query execution
	executionId, zerr = ath.StartQueryExecution(qCtx, dbName, query, outputLoc)
	if zerr != nil {
		return nil, zerr
	}
	// TODO write defer func for checking error and stopping query execution

	// get query info till it reaches terminal state
	for {
		status, zerr = ath.GetQueryInfo(qCtx, executionId)
		if zerr != nil {
			return nil, zerr
		}
		if status != "RUNNING" && status != "QUEUED" {
			break
		}
		// sleep for 2 secs
		time.Sleep(time.Second * 2)
	}

	// If status is not SUCCEEDED then fail
	if status != "SUCCEEDED" {
		return nil, zerror.NewZErr(zerror.ATHENA_QUERY_FAILED, fmt.Sprintf("failed with status %v", status))
	}

	// Get athena query output
	getQueryResultInput := athena.GetQueryResultsInput{}
	getQueryResultInput.SetQueryExecutionId(executionId)
	output, err := ath.athena.GetQueryResultsWithContext(ctx, &getQueryResultInput)
	if err != nil {
		return nil, zerror.NewZErr(http.StatusBadRequest, err.Error())
	}

	return output, nil
}

// StartQueryExecution is a wrapper around athena StartQueryExecutionWithContext
// and is used to run query on Athena. Function fills the input request and calls
// StartQueryExecutionWithContext.
func (ath *AWSAthena) StartQueryExecution(ctx context.Context,
	dbName string, query string, outputLoc string) (queryExecutionId string,
	zerr *zerror.ZError) {

	input := athena.StartQueryExecutionInput{}
	executionContext := athena.QueryExecutionContext{}
	executionContext.SetDatabase(dbName)
	resultConfig := athena.ResultConfiguration{}
	resultConfig.SetOutputLocation(outputLoc)
	input.SetQueryExecutionContext(&executionContext)
	input.SetQueryString(query)
	input.SetResultConfiguration(&resultConfig)
	output, err := ath.athena.StartQueryExecutionWithContext(ctx, &input)
	if err != nil {
		return "", zerror.NewZErr(zerror.ATHENA_QUERY_FAILED, err.Error())
	}
	return *output.QueryExecutionId, nil
}

// GetQueryInfo is a wrapper around athena.GetQueryExecutionWithContext. It returns
// the status, stats and reason for status change.
func (ath *AWSAthena) GetQueryInfo(ctx context.Context,
	queryExecutionId string) (string, *zerror.ZError) {

	input := athena.GetQueryExecutionInput{}
	input.SetQueryExecutionId(queryExecutionId)
	output, err := ath.athena.GetQueryExecutionWithContext(ctx, &input)
	if err != nil {
		return "", zerror.NewZErr(zerror.ATHENA_QUERY_FAILED, err.Error())
	}
	if output == nil || output.QueryExecution == nil ||
		output.QueryExecution.Status == nil || output.QueryExecution.Status.State == nil {
		return "", nil
	}
	return *output.QueryExecution.Status.State, nil
}
