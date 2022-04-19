package pipe

import (
	"fmt"

	"github.com/ThinkontrolSY/flux-builder/filter"
)

type FluxPipe struct {
	bucket  string
	start   string
	stop    *string
	filters []*filter.FluxFilter
}

func (p *FluxPipe) String() string {
	source := fmt.Sprintf("from(bucket: \"%s\")\n", p.bucket)
	var range_ string
	if p.stop == nil {
		range_ = fmt.Sprintf("|> range(start: %s)\n", p.start)
	} else {
		range_ = fmt.Sprintf("|> range(start: %s, stop: %s)\n", p.start, *p.stop)
	}

	return source + range_
}
