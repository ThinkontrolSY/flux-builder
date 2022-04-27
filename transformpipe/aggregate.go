package transformpipe

import (
	"fmt"
	"strings"
)

type TransformFn string

const (
	// Mean
	Mean TransformFn = "mean"
	// Min
	Min TransformFn = "min"
	// Max
	Max TransformFn = "max"
	// Sum
	Sum TransformFn = "sum"
	// Count
	Count TransformFn = "count"
	// Stddev
	Stddev TransformFn = "stddev"
	// Median
	Median TransformFn = "median"
	// First
	First TransformFn = "first"
	// Last
	Last TransformFn = "last"
	// Integral
	Integral TransformFn = "integral"
	// Mode
	Mode TransformFn = "mode"
	// Skew
	Skew TransformFn = "skew"
	// Spread
	Spread TransformFn = "spread"
	// Distinct
	Distinct TransformFn = "distinct"
	// Unique
	Unique TransformFn = "unique"
)

type AggregatorPipe struct {
	/*
		Duration of windows.
		Calendar months and years
		every supports all valid duration units, including calendar months (1mo) and years (1y).

		Aggregate by week
		When aggregating by week (1w), weeks are determined using the Unix epoch (1970-01-01T00:00:00Z UTC). The Unix epoch was on a Thursday, so all calculated weeks begin on Thursday.
	*/
	Every Duration

	/*
		period
		Duration of the window. Period is the length of each interval. The period can be negative, indicating the start and stop boundaries are reversed. Defaults to every value.
	*/
	Period *Duration

	/*
		column
		The column on which to operate. Defaults to "_value".
	*/
	Column *string

	TimeSrc *string

	TimeDst *string

	CreateEmpty *bool

	Fn TransformFn
}

func (a *AggregatorPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("fn: %s", a.Fn))
	if err := a.Every.Error(); err != nil {
		return "", err
	} else {
		params = append(params, fmt.Sprintf("every: %s", a.Every))
	}
	if a.Period != nil {
		if err := a.Period.Error(); err != nil {
			return "", err
		} else {
			params = append(params, fmt.Sprintf("period: %s", *a.Period))
		}
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf("column: \"%s\"", *a.Column))
	}
	if a.TimeSrc != nil {
		params = append(params, fmt.Sprintf("timeSrc: \"%s\"", *a.TimeSrc))
	}
	if a.TimeDst != nil {
		params = append(params, fmt.Sprintf("timeDst: \"%s\"", *a.TimeDst))
	}
	if a.CreateEmpty != nil {
		params = append(params, fmt.Sprintf("createEmpty: %t", *a.CreateEmpty))
	}
	return fmt.Sprintf("|> aggregateWindow(%s)", strings.Join(params, ", ")), nil
}
