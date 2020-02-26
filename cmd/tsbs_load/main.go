package main

import (
	"bytes"
	"fmt"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/source"
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	//err := viper.ReadInConfig()
	yamlExample := `
format: timescaledb
data-source:
  type: FILE
  file:
    location: 1
loader:
  runner:
    workers: 1
  db-specific:
    host: test-host
`
	err := viper.ReadConfig(bytes.NewBuffer([]byte(yamlExample)))
	if err != nil {
		panic(err)
	}
	topLevel := viper.GetViper()
	conf, target, err := ParseLoadConfig(topLevel)
	if err != nil {
		panic(err)
	}
	dataSource, err := source.NewDataSource(target, conf.DataSource)
	if err != nil {
		panic(err)
	}
	fmt.Println(dataSource)
}