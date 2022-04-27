package query

import (
	"fmt"
	"strings"

	"github.com/ThinkontrolSY/flux-builder/filter"
	pipe "github.com/ThinkontrolSY/flux-builder/transformpipe"
)

type FluxQuery struct {
	Bucket     string
	Start      string
	Stop       *string
	Filters    []*filter.FluxFilter
	Transforms []pipe.TransformPipe
}

func (q *FluxQuery) SetBucket(s string) *FluxQuery {
	q.Bucket = s
	return q
}

func (q *FluxQuery) SetStart(s string) *FluxQuery {
	q.Start = s
	return q
}

func (q *FluxQuery) SetStop(s *string) *FluxQuery {
	q.Stop = s
	return q
}

func (q *FluxQuery) AddFilter(f *filter.FluxFilter) *FluxQuery {
	if f != nil {
		q.Filters = append(q.Filters, f)
	}
	return q
}

func (q *FluxQuery) AddTransform(f pipe.TransformPipe) *FluxQuery {
	if f != nil {
		q.Transforms = append(q.Transforms, f)
	}
	return q
}

func (p *FluxQuery) QueryString() (string, error) {
	pipes := []string{
		fmt.Sprintf("from(bucket: \"%s\")", p.Bucket),
	}

	if p.Stop == nil {
		pipes = append(pipes, fmt.Sprintf("|> range(start: %s)", p.Start))
	} else {
		pipes = append(pipes, fmt.Sprintf("|> range(start: %s, stop: %s)", p.Start, *p.Stop))
	}

	for _, f := range p.Filters {
		if f == nil {
			continue
		}
		fp, err := f.Pipe()
		if err != nil {
			return "", err
		}
		pipes = append(pipes, fp)
	}

	for _, t := range p.Transforms {
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
