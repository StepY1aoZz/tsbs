package main

import (
	"os"
	"sync"
	"testing"
)

func TestFileScan(t *testing.T) {
	bytesPool = sync.Pool{
		New: func() interface{} {
			tmp := make([]byte, 0, 4*1024*1024)
			return &tmp
		},
	}
	f, err := os.Open("/tmp/gemini-data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ds := &fileDataSource{newFileDecoder(f)}
	point := ds.NextItem()
	b := &batch{}
	b.Append(point)
	point = ds.NextItem()
	b.Append(point)
}
