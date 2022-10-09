package handlers

import (
	"context"

	"metering/zenskar-project/meteringsrv/depend"
)

type Handlers struct {
	dep *depend.Dependency
}

// inits Handlers struct
func Init(ctx context.Context, dep *depend.Dependency) *Handlers {
	return &Handlers{dep: dep}
}
