package query

import (
	"fmt"
	"strings"

	"github.com/ThinkontrolSY/flux-builder/filter"
	pipe "github.com/ThinkontrolSY/flux-builder/transform_pipe"
)

type FluxQuery struct {
	bucket     string
	start      string
	stop       *string
	filters    []*filter.FluxFilter
	transforms []pipe.TransformPipe
}

func (q *FluxQuery) Bucket() string {
	return q.bucket
}

func (q *FluxQuery) Start() string {
	return q.start
}

func (q *FluxQuery) Stop() *string {
	return q.stop
}

func (q *FluxQuery) SetBucket(s string) *FluxQuery {
	q.bucket = s
	return q
}

func (q *FluxQuery) SetStart(s string) *FluxQuery {
	q.start = s
	return q
}

func (q *FluxQuery) SetStop(s *string) *FluxQuery {
	q.stop = s
	return q
}

func (q *FluxQuery) AddFilter(f *filter.FluxFilter) *FluxQuery {
	if f != nil {
		q.filters = append(q.filters, f)
	}
	return q
}

func (q *FluxQuery) AddTransform(f pipe.TransformPipe) *FluxQuery {
	if f != nil {
		q.transforms = append(q.transforms, f)
	}
	return q
}

func (p *FluxQuery) QueryString() (string, error) {
	pipes := []string{
		fmt.Sprintf("from(bucket: \"%s\")", p.bucket),
	}

	if p.stop == nil {
		pipes = append(pipes, fmt.Sprintf("|> range(start: %s)", p.start))
	} else {
		pipes = append(pipes, fmt.Sprintf("|> range(start: %s, stop: %s)", p.start, *p.stop))
	}

	for _, f := range p.filters {
		if f == nil {
			continue
		}
		fp, err := f.Pipe()
		if err != nil {
			return "", err
		}
		pipes = append(pipes, fp)
	}

	for _, t := range p.transforms {
		if t == nil {
			continue
		}
		tp, err := t.Pipe()
		if err != nil {
			return "", err
		}
		pipes = append(pipes, tp)
	}

	return strings.Join(pipes, "\n"), nil
}
