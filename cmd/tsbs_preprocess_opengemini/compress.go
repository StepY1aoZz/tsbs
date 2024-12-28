package main

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

var compressMethod = map[int]func([]byte) ([]byte, int, error){
	compressionSnappy: compressSnappy,
	compressionZstd:   compressZstd,
	compressionLz4:    compressLz4,
}

func doCompress(src []byte, compressType int) ([]byte, int, error) {
	switch compressType {
	case compressionNone:
		return src, len(src), nil
	default:
		return compressMethod[compression](src)
	}
}

func compressLz4(src []byte) ([]byte, int, error) {
	srcReader := bytes.NewBuffer(src)
	out := make([]byte, 0)
	outWriter := bytes.NewBuffer(out)
	zw := lz4.NewWriter(outWriter)
	defer zw.Close()
	n, err := io.Copy(zw, srcReader)
	if err != nil {
		return nil, 0, err
	}
	return out, int(n), nil
}

func compressSnappy(src []byte) ([]byte, int, error) {
	dst := snappy.Encode(nil, src)
	return dst, len(src), nil
}

func compressZstd(src []byte) ([]byte, int, error) {
	out := make([]byte, 0)
	outWriter := bytes.NewBuffer(out)
	enc, err := zstd.NewWriter(outWriter)
	if err != nil {
		return nil, 0, err
	}
	defer enc.Close()
	srcReader := bytes.NewBuffer(src)
	_, err = io.Copy(enc, srcReader)
	if err != nil {
		return nil, 0, err
	}
	return out, len(src), nil
}
