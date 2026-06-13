package main

import (
	"os"

	"github.com/lf-edge/ekuiper/sdk/go/api"
	sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
)

func main() {
	sdk.Start(os.Args, &sdk.PluginConfig{
		Name: "mongo",
		Sinks: map[string]sdk.NewSinkFunc{
			"mongoGo": func() api.Sink {
				return &mongoSink{}
			},
		},
	})
}
