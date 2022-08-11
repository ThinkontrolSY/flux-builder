package transformpipe

import (
	"fmt"
	"regexp"

	"github.com/mitchellh/mapstructure"
)

type TransformPipe interface {
	Pipe() (string, error)
}

type Duration string

func (d Duration) Error() error {
	r := regexp.MustCompile(`^(\d+(ns|us|ms|s|m|h|d|w|mo|y))+$`)
	if r.MatchString(string(d)) {
		return nil
	}
	return fmt.Errorf("Invalid duration value: %s, duration should format with IMPL#2026", d)
}

type TransformInput struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params"`
}

func (t *TransformInput) Transform() (TransformPipe, error) {
	switch t.Name {
	case "aggregateWindow":
		var tp AggregatorPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "bottom":
		var tp BottomPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "top":
		var tp TopPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "count":
		if t.Params == nil {
			return &CountPipe{}, nil
		}
		var tp CountPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "cumulativeSum":
		if t.Params == nil {
			return &CumulativeSumPipe{}, nil
		}
		var tp CumulativeSumPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "derivative":
		if t.Params == nil {
			return &DerivativePipe{}, nil
		}
		var tp DerivativePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "difference":
		if t.Params == nil {
			return &DifferencePipe{}, nil
		}
		var tp DifferencePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "distinct":
		if t.Params == nil {
			return &DistinctPipe{}, nil
		}
		var tp DistinctPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "doubleEMA":
		var tp DoubleEMAPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "elapsed":
		if t.Params == nil {
			return &ElapsedPipe{}, nil
		}
		var tp ElapsedPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "exponentialMovingAverage":
		var tp ExponentialMovingAveragePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "fill":
		var tp FillPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "first":
		return &FirstPipe{}, nil
	case "group":
		if t.Params == nil {
			return &GroupPipe{}, nil
		}
		var tp GroupPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "last":
		return &LastPipe{}, nil
	case "increase":
		if t.Params == nil {
			return &IncreasePipe{}, nil
		}
		var tp IncreasePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "integral":
		var tp IntegralPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "kaufmansAMA":
		var tp KaufmansAMAPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "kaufmansER":
		var tp KaufmansERPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "limit":
		var tp LimitPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "max":
		if t.Params == nil {
			return &MaxPipe{}, nil
		}
		var tp MaxPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "mean":
		if t.Params == nil {
			return &MeanPipe{}, nil
		}
		var tp MeanPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "median":
		if t.Params == nil {
			return &MedianPipe{}, nil
		}
		var tp MedianPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "min":
		if t.Params == nil {
			return &MinPipe{}, nil
		}
		var tp MinPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "mode":
		if t.Params == nil {
			return &ModePipe{}, nil
		}
		var tp ModePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "movingAverage":
		var tp MovingAveragePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "quantile":
		var tp QuantilePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "relativeStrengthIndex":
		var tp RelativeStrengthIndexPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "skew":
		if t.Params == nil {
			return &SkewPipe{}, nil
		}
		var tp SkewPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "spread":
		if t.Params == nil {
			return &SpreadPipe{}, nil
		}
		var tp SpreadPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "sort":
		if t.Params == nil {
			return &SortPipe{}, nil
		}
		var tp SortPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "stddev":
		if t.Params == nil {
			return &StddevPipe{}, nil
		}
		var tp StddevPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "stateCount":
		var tp StateCountPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "sum":
		if t.Params == nil {
			return &SumPipe{}, nil
		}
		var tp SumPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "timeShift":
		var tp TimeShiftPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "stateDuration":
		var tp StateDurationPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "tail":
		var tp TailPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "timeMovingAverage":
		var tp TimeMovingAveragePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "timeWeightedAvg":
		var tp TimeWeightedAvgPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "toBool":
		return &ToBoolPipe{}, nil
	case "toFloat":
		return &ToFloatPipe{}, nil
	case "toInt":
		return &ToIntPipe{}, nil
	case "toString":
		return &ToStringPipe{}, nil
	case "toTime":
		return &ToTimePipe{}, nil
	case "toUInt":
		return &ToUIntPipe{}, nil
	case "tripleEMA":
		var tp TripleEMAPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "tripleExponentialDerivative":
		var tp TripleExponentialDerivativePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "truncateTimeColumn":
		var tp TruncateTimeColumnPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "unique":
		if t.Params == nil {
			return &UniquePipe{}, nil
		}
		var tp UniquePipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "window":
		var tp WindowPipe
		if err := mapstructure.Decode(t.Params, &tp); err == nil {
			return &tp, nil
		} else {
			return nil, err
		}
	case "yield":
		return &YieldPipe{}, nil
	default:
		return nil, fmt.Errorf("Invalid transform name: %s", t.Name)
	}
}
