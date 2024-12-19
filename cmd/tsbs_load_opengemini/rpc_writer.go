package main

import (
	"context"
	"github.com/timescale/tsbs/pkg/targets/opengemini/proto"
	"google.golang.org/grpc"
	"log"
)

type rpcWriter struct {
	url    string
	client proto.WriteServiceClient
}

type rpcWriteConfig struct {
	Measurement     string
	DBName          string
	RetentionPolicy string
}

func (w *rpcWriter) WriteRequest(cfg rpcWriteConfig, data []byte) error {
	req := &proto.WriteRequest{
		Database:        cfg.DBName,
		RetentionPolicy: cfg.RetentionPolicy,
		Records: []*proto.Record{
			{
				Measurement:    cfg.Measurement,
				CompressMethod: proto.CompressMethod_UNCOMPRESSED,
				Block:          data,
			},
		},
	}
	if w == nil {
		log.Fatalf("client not initialized")
	}
	res, err := w.client.Write(context.Background(), req)
	if err != nil {
		return err
	}
	if res.Code != proto.ResponseCode_Success {
		log.Fatalf("internal error when writing")
	}
	return nil
}

func (w *rpcWriter) Init(curr int) error {
	w.url = daemonURLs[1+(curr%(len(daemonURLs)-1))]
	con, err := grpc.NewClient(w.url, grpc.WithInsecure())
	if err != nil {
		return err
	}
	w.client = proto.NewWriteServiceClient(con)
	res, err := w.client.Ping(context.Background(), &proto.PingRequest{})
	if err != nil {
		return err
	}
	if res.Status == proto.ServerStatus_Up {
		log.Printf("rpc server is up")
	}
	return nil
}
