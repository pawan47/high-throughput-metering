package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	zerror "metering/zenskar-project/error"
)

func (h *Handlers) AboutMe(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(("About Me, please use /bill")))
	w.WriteHeader(http.StatusOK)
	log.Println("About Me triggered")
}

// Meter is a HTTP handler which handles request for /meter endpoint
func (h *Handlers) Meter(w http.ResponseWriter, r *http.Request) {
	log.Println("Meter Handler triggered")
	ctx := context.Background()
	var zerr *zerror.ZError

	// Based on method call respective functions
	switch r.Method {
	case http.MethodGet:
		zerr = h.GetBillingStats(ctx, w, r)
	case http.MethodPost:
		zerr = h.AddConsumption(ctx, w, r)
	default:
		zerr = zerror.NewZErr(zerror.UNSUPPORTED_HTTP_METHOD, "unsupported http method called")
	}
	if zerr != nil {
		// TODO: add proper status in HTTP.
		log.Printf("Encountered error for request %+v, with error %v\n", r, zerr.Error())
		http.Error(w, zerr.Error(), http.StatusBadRequest)
	}

	return
}

// AddConsumptionParams is body for adding consumption
type AddConsumptionParams struct {
	// ID is the customer ID
	ID string `json:"id"`
	// Bytes is number of bytes to meter
	Bytes int64 `json:"bytes"`
	// MeterTimeEpoch is sent by consumer, This dictates the event time, It can be in past
	MeterTimeEpoch int64 `json:"meter_time_epoch"`
}

// validate validates AddConsumptionParams request
func (a *AddConsumptionParams) validate() *zerror.ZError {
	if a.ID == "" {
		return zerror.NewZErr(zerror.INVALID_REQUEST, "id cannot be nil")
	} else if a.Bytes == 0 {
		return zerror.NewZErr(zerror.INVALID_REQUEST, "bytes cannot be 0")
	} else if a.MeterTimeEpoch == 0 {
		return zerror.NewZErr(zerror.INVALID_REQUEST, "timeepoch of metered event cannot be 0")
	}
	return nil
}

// marshall marshals AddConsumptionParams struct
func (a *AddConsumptionParams) marshall() ([]byte, *zerror.ZError) {
	marshalledByte, err := json.Marshal(a)
	if err != nil {
		return nil, zerror.NewZErr(zerror.MARSHALL_FAILED, err.Error())
	}
	return marshalledByte, nil
}

// AddConsumption Puts meter data into firehouse.
func (h *Handlers) AddConsumption(ctx context.Context, w http.ResponseWriter,
	r *http.Request) *zerror.ZError {
	log.Printf("Add AddConsumption triggered with request body %+v", r.Body)
	params := new(AddConsumptionParams)
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		return zerror.NewZErr(zerror.HTTP_DECODE_ERR, err.Error())
	}

	// Put data into data stream(firehose)
	zerr := h.PutDataToDataStream(ctx, params)
	if zerr != nil {
		return zerr
	}

	// write header status
	w.WriteHeader(http.StatusNoContent)

	return nil
}

// GetBillingStatsRes is response json struct for GetBillingStats
type GetBillingStatsRes struct {
	TotalMeteredData int64 `json:"total_metered_data"`
}

// marshall marhsalls GetBillingStatsRes
func (g *GetBillingStatsRes) marshall() ([]byte, *zerror.ZError) {
	marshalledByte, err := json.Marshal(g)
	if err != nil {
		return nil, zerror.NewZErr(zerror.MARSHALL_FAILED, err.Error())
	}
	return marshalledByte, nil
}

// GetBillingStats gets billing stats
func (h *Handlers) GetBillingStats(ctx context.Context, w http.ResponseWriter,
	r *http.Request) *zerror.ZError {
	log.Printf("GetBillingStats handler triggered with request url %+v", r.URL)

	// Parse query parameters
	custID, timestampEpochGt, timestampEpochLs, zerr := parseGetBillingStatsQParams(r)
	if zerr != nil {
		return zerr
	}

	// Get stats using athena given query parameter
	totalBytesConsumed, zerr := h.GetTotalBytesFromAthena(ctx, custID, timestampEpochGt, timestampEpochLs)
	if zerr != nil {
		return zerr
	}
	res := &GetBillingStatsRes{TotalMeteredData: totalBytesConsumed}
	marshalledRes, zerr := res.marshall()
	if zerr != nil {
		return zerr
	}

	// write data into json format and return
	w.Header().Set("content-Type", "application/json")
	w.Write(marshalledRes)
	w.WriteHeader(http.StatusOK)
	return nil
}

// parseGetBillingStatsQParams parses parameters from query params for GET method
// returns:
// custID - string
// timestampEpochGt - int
// timestampEpochLs - int
// zerr - zenskar error
func parseGetBillingStatsQParams(r *http.Request) (string, int64, int64, *zerror.ZError) {
	var (
		queryParams      = r.URL.Query()
		timestampEpochGt int
		timestampEpochLs int
		err              error
	)

	// CustID is mandatory field
	custID := queryParams.Get("id")
	if custID == "" {
		return "", 0, 0, zerror.NewZErr(zerror.INVALID_REQUEST,
			"customer ID is required for getting stats")
	}

	// get greater than time epoch
	timestampEpochGtStr := queryParams.Get("time_epoch_greater")
	if timestampEpochGtStr != "" {
		timestampEpochGt, err = strconv.Atoi(timestampEpochGtStr)
		if err != nil {
			return "", 0, 0, zerror.NewZErr(zerror.INVALID_REQUEST, "time_epoch_greater must be integer")
		}
	}

	// get less than time epoch
	timestampEpochLessStr := queryParams.Get("time_epoch_less")
	if timestampEpochLessStr != "" {
		timestampEpochLs, err = strconv.Atoi(timestampEpochLessStr)
		if err != nil {
			return "", 0, 0, zerror.NewZErr(zerror.INVALID_REQUEST, "time_epoch_less must be integer")
		}
	}

	return custID, int64(timestampEpochGt), int64(timestampEpochLs), nil
}
