// bulk_load_influx loads an InfluxDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"github.com/timescale/tsbs/pkg/targets/opengemini/proto"
)

// Program option vars:
var (
	daemonURLs        []string
	replicationFactor int
)

// Global vars
var (
	loader         load.BenchmarkRunner
	config         load.BenchmarkRunnerConfig
	bytesPool      sync.Pool
	target         targets.ImplementedTarget
	maxSize        int
	fileName       string
	compressMethod int
	compressDict   = map[int]proto.CompressMethod{
		compressionNone:   proto.CompressMethod_UNCOMPRESSED,
		compressionSnappy: proto.CompressMethod_SNAPPY,
		compressionZstd:   proto.CompressMethod_ZSTD_FAST,
		compressionLz4:    proto.CompressMethod_LZ4_FAST,
	}
)

const (
	compressionNone = iota
	compressionSnappy
	compressionZstd
	compressionLz4
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatOpenGemini)
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	var csvDaemonURLs string

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonURLs = viper.GetString("urls")
	maxSize = viper.GetInt("maxsize")

	if config.BatchSize != 1 {
		log.Printf("batch size must be 1 for openGemini record-writing benchmark, i.e. only one record will be proceesed in one request")
		config.BatchSize = 1
	}

	daemonURLs = strings.Split(csvDaemonURLs, ",")
	if len(daemonURLs) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	if len(daemonURLs) < 2 {
		log.Fatalf("at least two urls must be specified,for creating db and writing separately")
	}
	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
	fileName = config.FileName
	log.Printf("openGemini record-writing benchmark started")
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	var f *os.File
	if len(fileName) == 0 {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(fileName)
		if err != nil {
			log.Fatalf("unable to open file %s : %v", fileName, err)
		}
	}
	t := &fileDataSource{newFileDecoder(f)}
	return t
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bytesPool = sync.Pool{
		New: func() interface{} {
			t := make([]byte, 0, 4*1024*1024)
			return &t
		},
	}

	loader.RunBenchmark(&benchmark{})
}
