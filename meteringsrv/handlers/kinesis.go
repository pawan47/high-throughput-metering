package handlers

import (
	"context"

	zerror "metering/zenskar-project/error"
)

// PutDataToDataStream puts data into firehose
func (h *Handlers) PutDataToDataStream(ctx context.Context, req *AddConsumptionParams) *zerror.ZError {
	// validates the request
	zerr := req.validate()
	if zerr != nil {
		return zerr
	}

	// marshall json data
	data, zerr := req.marshall()
	if zerr != nil {
		return zerr
	}

	// put data into firehose
	return h.dep.Kinesis.PutRecord(ctx, data, h.dep.KinesisStreamName)
}
