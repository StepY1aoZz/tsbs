package opengemini

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &openGeminiTarget{}
}

type openGeminiTarget struct {
}

func (t *openGeminiTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086,http://localhost:8305", "openGemini gRPC URLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Int("maxsize", 5<<20, "Maximum size in bytes of a single record")
}

func (t *openGeminiTarget) TargetName() string {
	return constants.FormatOpenGemini
}

func (t *openGeminiTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *openGeminiTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
