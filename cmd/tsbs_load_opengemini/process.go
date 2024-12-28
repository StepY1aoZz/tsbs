package main

import (
	"log"

	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	rpcWriter *rpcWriter
}

func (p *processor) Init(workerNum int, _, _ bool) {
	p.rpcWriter = &rpcWriter{}
	log.Printf("initing processor...")
	err := p.rpcWriter.Init(workerNum)
	if err != nil {
		log.Fatal(err.Error())
	}
	if p.rpcWriter.client == nil {
		log.Fatal("rpcWriter client not initialized")
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	tb := b.(*batch)
	if doLoad {
		c := rpcWriteConfig{
			DBName:          loader.DatabaseName(),
			Measurement:     tb.mst,
			RetentionPolicy: "",
		}
		err := p.rpcWriter.WriteRequest(c, tb.data)
		if err != nil {
			log.Printf("error writing request: %v", err)
		}
	}
	tb.data = tb.data[:0]
	bytesPool.Put(&tb.data)
	return uint64(tb.metric), uint64(tb.row)
}
