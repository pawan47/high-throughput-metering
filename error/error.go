package error

import (
	"encoding/json"
	"fmt"
	"log"
)

// ZError is zenskar error internal error code management
type ZError struct {
	Code     int64  `json:"code"`
	ErrorStr string `json:"error"`
}

func (z *ZError) Error() string {
	zErrByte, err := json.Marshal(z)
	if err != nil {
		// THIS should never happen
		log.Fatal(fmt.Sprintf("failed to marhsall error with %+v", err))
	}
	return string(zErrByte)
}

func NewZErr(errCode int64, errorStr string) *ZError {
	return &ZError{
		Code:     errCode,
		ErrorStr: errorStr,
	}
}

type ErrorCodes int64

const (
	INVALID_REQUEST         = 1
	KINESIS_PUT_FAILED      = 2
	MARSHALL_FAILED         = 3
	UNSUPPORTED_HTTP_METHOD = 4
	HTTP_DECODE_ERR         = 5
	ATHENA_QUERY_FAILED     = 6
)
