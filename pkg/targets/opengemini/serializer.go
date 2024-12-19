package opengemini

import (
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

type Serializer struct {
}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	return nil
}

func (s *Serializer) doWrite(w io.Writer) error {
	return nil
}
