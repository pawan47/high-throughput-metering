package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	zerror "metering/zenskar-project/error"
)

// GetTotalBytesFromAthena runs athena query to get data total number of bytes for an org
func (h *Handlers) GetTotalBytesFromAthena(ctx context.Context, custID string,
	timestampEpochGt int64, timestampEpochLs int64) (int64, *zerror.ZError) {

	// execute the query based on parameter given
	output, zerr := h.dep.AWSAthena.ExecuteQuerySync(ctx, h.dep.GlueDBName,
		formStatsQuery(custID, timestampEpochGt, timestampEpochLs),
		h.dep.AthenaOutputLoc, time.Minute)
	if zerr != nil {
		return 0, zerr
	}

	// If output is nil or result set is nil then there is something wrong with athena layer
	// hence return error
	if output == nil || output.ResultSet == nil {
		return 0, zerror.NewZErr(http.StatusBadRequest, "GetQueryResultsWithContext"+
			" output or result set cannot be nil")
	}

	// TODO add proper parsing of result of athena, currently hard coded
	if len(output.ResultSet.Rows) != 2 {
		return 0, zerror.NewZErr(http.StatusBadRequest, "numbers of rows must 1 "+
			"as we are running sum query to get sum of all rows")
	}
	charVal := output.ResultSet.Rows[1].Data[0].VarCharValue
	if charVal == nil {
		return 0, nil
	}

	totalBytes, err := strconv.ParseInt(*charVal, 10,
		64)
	if err != nil {
		return 0, zerror.NewZErr(http.StatusBadRequest, err.Error())
	}

	return totalBytes, nil
}

// formStatsQuery forms athena query based on query params
func formStatsQuery(custID string, timestampEpochGt int64, timestampEpochLs int64) string {
	qStr := "select sum(t1.bytes) from billingdatazenskar as t1"
	args := make([]string, 0)
	if custID != "" {
		args = append(args, fmt.Sprintf("t1.id = '%s'", custID))
	}
	if timestampEpochGt > 0 {
		args = append(args, fmt.Sprintf("t1.meter_time_epoch >= %d", timestampEpochGt))
	}
	if timestampEpochLs > 0 {
		args = append(args, fmt.Sprintf("t1.meter_time_epoch <= %d", timestampEpochLs))
	}
	if len(args) != 0 {
		qStr = fmt.Sprintf("%s WHERE %s", qStr, strings.Join(args, " AND "))
	}
	return qStr
}
