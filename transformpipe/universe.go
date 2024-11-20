package transformpipe

import (
	"fmt"
	"strings"
)

type BottomPipe struct {
	N       int
	Columns []string
}

func (a *BottomPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}

	return fmt.Sprintf("|> bottom(%s)", strings.Join(params, ", ")), nil
}

type TopPipe struct {
	N       int
	Columns []string
}

func (a *TopPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}

	return fmt.Sprintf("|> top(%s)", strings.Join(params, ", ")), nil
}

type CountPipe struct {
	Column *string
}

func (a *CountPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> count(column: "%s")`, *a.Column), nil
	}
	return "|> count()", nil
}

type CumulativeSumPipe struct {
	Columns []string
}

func (a *CumulativeSumPipe) Pipe() (string, error) {
	if len(a.Columns) > 0 {
		return fmt.Sprintf(`|> cumulativeSum(columns: ["%s"])`, strings.Join(a.Columns, `", "`)), nil
	}

	return "|> cumulativeSum()", nil
}

type DerivativePipe struct {
	Unit        *Duration
	NonNegative *bool
	Columns     []string
	TimeColumn  *string
}

func (a *DerivativePipe) Pipe() (string, error) {
	var params []string
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	if a.TimeColumn != nil {
		params = append(params, fmt.Sprintf(`timeColumn: "%s"`, *a.TimeColumn))
	}
	if a.Unit != nil {
		if err := a.Unit.Error(); err != nil {
			return "", err
		} else {
			params = append(params, fmt.Sprintf("unit: %s", *a.Unit))
		}
	}
	if a.NonNegative != nil {
		params = append(params, fmt.Sprintf("nonNegative: %t", *a.NonNegative))
	}

	return fmt.Sprintf("|> derivative(%s)", strings.Join(params, ", ")), nil

}

type DifferencePipe struct {
	NonNegative *bool
	Columns     []string
	KeepFirst   *bool
	InitialZero *bool
}

func (a *DifferencePipe) Pipe() (string, error) {
	var params []string
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	if a.KeepFirst != nil {
		params = append(params, fmt.Sprintf("keepFirst: %t", *a.KeepFirst))
	}
	if a.InitialZero != nil {
		params = append(params, fmt.Sprintf("initialZero: %t", *a.InitialZero))
	}
	if a.NonNegative != nil {
		params = append(params, fmt.Sprintf("nonNegative: %t", *a.NonNegative))
	}

	return fmt.Sprintf("|> difference(%s)", strings.Join(params, ", ")), nil

}

type DistinctPipe struct {
	Column *string
}

func (a *DistinctPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> distinct(column: "%s")`, *a.Column), nil
	}
	return "|> distinct()", nil
}

type DoubleEMAPipe struct {
	N int
}

func (a *DoubleEMAPipe) Pipe() (string, error) {
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	return fmt.Sprintf("|> doubleEMA(n: %d)", a.N), nil
}

type ElapsedPipe struct {
	Unit       *Duration
	ColumnName *string
	TimeColumn *string
}

func (a *ElapsedPipe) Pipe() (string, error) {
	var params []string
	if a.TimeColumn != nil {
		params = append(params, fmt.Sprintf(`timeColumn: "%s"`, *a.TimeColumn))
	}
	if a.Unit != nil {
		if err := a.Unit.Error(); err != nil {
			return "", err
		} else {
			params = append(params, fmt.Sprintf("unit: %s", *a.Unit))
		}
	}
	if a.ColumnName != nil {
		params = append(params, fmt.Sprintf(`columnName: "%s"`, *a.ColumnName))
	}

	return fmt.Sprintf("|> elapsed(%s)", strings.Join(params, ", ")), nil

}

type ExponentialMovingAveragePipe struct {
	N int
}

func (a *ExponentialMovingAveragePipe) Pipe() (string, error) {
	return fmt.Sprintf("|> exponentialMovingAverage(n: %d)", a.N), nil
}

type FilterPipe struct {
	Fn string
}

func (a *FilterPipe) Pipe() (string, error) {
	return fmt.Sprintf("|> filter(fn: %s)", a.Fn), nil
}

type FillPipe struct {
	Value       interface{}
	Column      *string
	UsePrevious *bool
}

func (a *FillPipe) Pipe() (string, error) {
	var params []string
	if a.UsePrevious != nil && *a.UsePrevious == true {
		params = append(params, "usePrevious: true")
	} else if a.Value != nil {
		switch a.Value.(type) {
		case string:
			params = append(params, fmt.Sprintf(`value: "%s"`, a.Value.(string)))
		case int:
			params = append(params, fmt.Sprintf(`value: %d`, a.Value.(int)))
		case float64:
			params = append(params, fmt.Sprintf(`value: %f`, a.Value.(float64)))
		case bool:
			params = append(params, fmt.Sprintf(`value: %t`, a.Value.(bool)))
		default:
			return "", fmt.Errorf("unsupported value type: %T", a.Value)
		}
	}

	if len(params) == 0 {
		return "", fmt.Errorf("fill requires at least one parameter")
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> fill(%s)", strings.Join(params, ", ")), nil

}

type FirstPipe struct{}

func (a *FirstPipe) Pipe() (string, error) {
	return "|> first()", nil
}

type GroupPipe struct {
	Mode    *string
	Columns []string
}

func (a *GroupPipe) Pipe() (string, error) {
	var params []string
	if a.Mode != nil {
		mode := "by"
		if *a.Mode == "except" {
			mode = "except"
		}
		params = append(params, fmt.Sprintf(`mode: "%s"`, mode))
	}
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	return fmt.Sprintf("|> group(%s)", strings.Join(params, ", ")), nil
}

type LastPipe struct{}

func (a *LastPipe) Pipe() (string, error) {
	return "|> last()", nil
}

type IncreasePipe struct {
	Columns []string
}

func (a *IncreasePipe) Pipe() (string, error) {
	if len(a.Columns) > 0 {
		return fmt.Sprintf(`|> increase(columns: ["%s"])`, strings.Join(a.Columns, `", "`)), nil
	}

	return "|> increase()", nil
}

type IntegralPipe struct {
	Unit        Duration
	Column      *string
	TimeColumn  *string
	Interpolate *string
}

func (a *IntegralPipe) Pipe() (string, error) {
	var params []string
	if a.TimeColumn != nil {
		params = append(params, fmt.Sprintf(`timeColumn: "%s"`, *a.TimeColumn))
	}
	if err := a.Unit.Error(); err != nil {
		return "", err
	} else {
		params = append(params, fmt.Sprintf("unit: %s", a.Unit))
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	if a.Interpolate != nil {
		params = append(params, fmt.Sprintf(`interpolate: "%s"`, *a.Interpolate))
	}

	return fmt.Sprintf("|> integral(%s)", strings.Join(params, ", ")), nil
}

type KaufmansAMAPipe struct {
	N      int
	Column *string
}

func (a *KaufmansAMAPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> kaufmansAMA(%s)", strings.Join(params, ", ")), nil
}

type KaufmansERPipe struct {
	N int
}

func (a *KaufmansERPipe) Pipe() (string, error) {
	return fmt.Sprintf("|> kaufmansER(n: %d)", a.N), nil
}

type LimitPipe struct {
	N      int
	Offset *int
}

func (a *LimitPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if a.Offset != nil {
		params = append(params, fmt.Sprintf("offset: %d", *a.Offset))
	}
	return fmt.Sprintf("|> limit(%s)", strings.Join(params, ", ")), nil
}

type MaxPipe struct {
	Column *string
}

func (a *MaxPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> max(column: "%s")`, *a.Column), nil
	}
	return "|> max()", nil
}

type MinPipe struct {
	Column *string
}

func (a *MinPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> min(column: "%s")`, *a.Column), nil
	}
	return "|> min()", nil
}

type ModePipe struct {
	Column *string
}

func (a *ModePipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> mode(column: "%s")`, *a.Column), nil
	}
	return "|> mode()", nil
}

type MeanPipe struct {
	Column *string
}

func (a *MeanPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> mean(column: "%s")`, *a.Column), nil
	}
	return "|> mean()", nil
}

type Estimate string

const (
	EstimateTdigest  Estimate = "estimate_tdigest"
	EstimateMean     Estimate = "exact_mean"
	EstimateSelector Estimate = "estimate_selector"
)

type MedianPipe struct {
	Column      *string
	Method      *Estimate
	Compression *float64
}

func (a *MedianPipe) Pipe() (string, error) {
	var params []string
	if a.Method != nil {
		params = append(params, fmt.Sprintf(`method: "%s"`, *a.Method))
	}
	if a.Compression != nil {
		params = append(params, fmt.Sprintf(`compression: %f`, *a.Compression))
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> median(%s)", strings.Join(params, ", ")), nil
}

type MovingAveragePipe struct {
	N int
}

func (a *MovingAveragePipe) Pipe() (string, error) {
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	return fmt.Sprintf("|> movingAverage(n: %d)", a.N), nil
}

type QuantilePipe struct {
	Q           float64
	Column      *string
	Method      *Estimate
	Compression *float64
}

func (a *QuantilePipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf("q: %f", a.Q))
	if a.Method != nil {
		params = append(params, fmt.Sprintf(`method: "%s"`, *a.Method))
	}
	if a.Compression != nil {
		params = append(params, fmt.Sprintf(`compression: %f`, *a.Compression))
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> quantile(%s)", strings.Join(params, ", ")), nil
}

type RelativeStrengthIndexPipe struct {
	N       int
	Columns []string
}

func (a *RelativeStrengthIndexPipe) Pipe() (string, error) {
	var params []string
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	return fmt.Sprintf("|> relativeStrengthIndex(%s)", strings.Join(params, ", ")), nil
}

type SkewPipe struct {
	Column *string
}

func (a *SkewPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> skew(column: "%s")`, *a.Column), nil
	}
	return "|> skew()", nil
}

type SortPipe struct {
	Columns []string
	Desc    *bool
}

func (a *SortPipe) Pipe() (string, error) {
	var params []string
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	if a.Desc != nil {
		params = append(params, fmt.Sprintf(`desc: %t`, *a.Desc))
	}
	return fmt.Sprintf("|> sort(%s)", strings.Join(params, ", ")), nil
}

type SpreadPipe struct {
	Column *string
}

func (a *SpreadPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> spread(column: "%s")`, *a.Column), nil
	}
	return "|> spread()", nil
}

type StateCountPipe struct {
	Column *string
	Fn     string
}

func (a *StateCountPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf(`fn: %s`, a.Fn))
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> stateCount(%s)", strings.Join(params, ", ")), nil
}

type StateDurationPipe struct {
	Column *string
	Fn     string
	Unit   *Duration
}

func (a *StateDurationPipe) Pipe() (string, error) {
	var params []string
	params = append(params, fmt.Sprintf(`fn: %s`, a.Fn))
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	if a.Unit != nil {
		if err := a.Unit.Error(); err != nil {
			return "", err
		} else {
			params = append(params, fmt.Sprintf("unit: %s", *a.Unit))
		}
	}
	return fmt.Sprintf("|> stateDuration(%s)", strings.Join(params, ", ")), nil
}

type StddevMode string // "population" or "sample"
const (
	StddevModePopulation StddevMode = "population"
	StddevModeSample     StddevMode = "sample"
)

type StddevPipe struct {
	Column *string
	Mode   *StddevMode
}

func (a *StddevPipe) Pipe() (string, error) {
	var params []string
	if a.Mode != nil {
		params = append(params, fmt.Sprintf(`mode: "%s"`, *a.Mode))
	}
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> stddev(%s)", strings.Join(params, ", ")), nil
}

type SumPipe struct {
	Column *string
}

func (a *SumPipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> sum(column: "%s")`, *a.Column), nil
	}
	return "|> sum()", nil
}

type TailPipe struct {
	N      int
	Offset *int
}

func (a *TailPipe) Pipe() (string, error) {
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	var params []string
	params = append(params, fmt.Sprintf("n: %d", a.N))
	if a.Offset != nil {
		params = append(params, fmt.Sprintf("offset: %d", *a.Offset))
	}
	return fmt.Sprintf("|> tail(%s)", strings.Join(params, ", ")), nil
}

type TimeMovingAveragePipe struct {
	Every  Duration
	Period Duration
	Column *string
}

func (a *TimeMovingAveragePipe) Pipe() (string, error) {
	if err := a.Every.Error(); err != nil {
		return "", err
	} else if err := a.Period.Error(); err != nil {
		return "", err
	}
	var params []string
	params = append(params, fmt.Sprintf("every: %s", a.Every))
	params = append(params, fmt.Sprintf("period: %s", a.Period))
	if a.Column != nil {
		params = append(params, fmt.Sprintf(`column: "%s"`, *a.Column))
	}
	return fmt.Sprintf("|> timeMovingAverage(%s)", strings.Join(params, ", ")), nil
}

type TimeShiftPipe struct {
	Duration Duration
	Columns  []string
}

func (a *TimeShiftPipe) Pipe() (string, error) {
	if err := a.Duration.Error(); err != nil {
		return "", err
	}
	var params []string
	params = append(params, fmt.Sprintf("duration: %s", a.Duration))
	if len(a.Columns) > 0 {
		params = append(params, fmt.Sprintf(`columns: ["%s"]`, strings.Join(a.Columns, `", "`)))
	}
	return fmt.Sprintf("|> timeShift(%s)", strings.Join(params, ", ")), nil
}

type KeepPipe struct {
	Columns []string
}

func (a *KeepPipe) Pipe() (string, error) {
	if len(a.Columns) > 0 {
		return fmt.Sprintf(`|> keep(columns: ["%s"])`, strings.Join(a.Columns, `", "`)), nil
	}
	return "", fmt.Errorf("keep requires at least one column")
}

type DropPipe struct {
	Columns []string
}

func (a *DropPipe) Pipe() (string, error) {
	if len(a.Columns) > 0 {
		return fmt.Sprintf(`|> drop(columns: ["%s"])`, strings.Join(a.Columns, `", "`)), nil
	}
	return "", fmt.Errorf("drop requires at least one column")
}

type TimeWeightedAvgPipe struct {
	Unit Duration
}

func (a *TimeWeightedAvgPipe) Pipe() (string, error) {
	if err := a.Unit.Error(); err != nil {
		return "", err
	}
	return fmt.Sprintf("|> timeWeightedAvg(unit: %s)", a.Unit), nil
}

type ToBoolPipe struct{}

func (a *ToBoolPipe) Pipe() (string, error) {
	return "|> toBool()", nil
}

type ToFloatPipe struct{}

func (a *ToFloatPipe) Pipe() (string, error) {
	return "|> toFloat()", nil
}

type ToStringPipe struct{}

func (a *ToStringPipe) Pipe() (string, error) {
	return "|> toString()", nil
}

type ToIntPipe struct{}

func (a *ToIntPipe) Pipe() (string, error) {
	return "|> toInt()", nil
}

type ToTimePipe struct{}

func (a *ToTimePipe) Pipe() (string, error) {
	return "|> toTime()", nil
}

type ToUIntPipe struct{}

func (a *ToUIntPipe) Pipe() (string, error) {
	return "|> toUInt()", nil
}

type TripleEMAPipe struct {
	N int
}

func (a *TripleEMAPipe) Pipe() (string, error) {
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	return fmt.Sprintf("|> tripleEMA(n: %d)", a.N), nil
}

type TripleExponentialDerivativePipe struct {
	N int
}

func (a *TripleExponentialDerivativePipe) Pipe() (string, error) {
	if a.N <= 0 {
		return "", fmt.Errorf("n must be greater than 0")
	}
	return fmt.Sprintf("|> tripleExponentialDerivative(n: %d)", a.N), nil
}

type TruncateTimeColumnPipe struct {
	Unit Duration
}

func (a *TruncateTimeColumnPipe) Pipe() (string, error) {
	if err := a.Unit.Error(); err != nil {
		return "", err
	}
	return fmt.Sprintf("|> truncateTimeColumn(unit: %s)", a.Unit), nil
}

type UniquePipe struct {
	Column *string
}

func (a *UniquePipe) Pipe() (string, error) {
	if a.Column != nil {
		return fmt.Sprintf(`|> unique(column: "%s")`, *a.Column), nil
	}
	return "|> unique()", nil
}

type WindowPipe struct {
	Every       *Duration
	Period      *Duration
	Offset      *Duration
	TimeColumn  *string
	StartColumn *string
	StopColumn  *string
	Location    *string
	CreateEmpty *bool
}

func (a *WindowPipe) Pipe() (string, error) {
	var params []string
	if a.Every != nil {
		params = append(params, fmt.Sprintf("every: %s", *a.Every))
	}
	if a.Period != nil {
		params = append(params, fmt.Sprintf("period: %s", *a.Period))
	}
	if len(params) == 0 {
		return "", fmt.Errorf("window function requires at least one of \"every\" or \"period\" to be set and non-zero")
	}
	if a.Offset != nil {
		params = append(params, fmt.Sprintf("offset: %s", *a.Offset))
	}
	if a.TimeColumn != nil {
		params = append(params, fmt.Sprintf(`timeColumn: "%s"`, *a.TimeColumn))
	}
	if a.StartColumn != nil {
		params = append(params, fmt.Sprintf(`startColumn: "%s"`, *a.StartColumn))
	}
	if a.StopColumn != nil {
		params = append(params, fmt.Sprintf(`stopColumn: "%s"`, *a.StopColumn))
	}
	if a.Location != nil {
		params = append(params, fmt.Sprintf(`location: "%s"`, *a.Location))
	}
	if a.CreateEmpty != nil {
		params = append(params, fmt.Sprintf(`createEmpty: %t`, *a.CreateEmpty))
	}
	return fmt.Sprintf("|> window(%s)", strings.Join(params, ", ")), nil
}

type YieldPipe struct {
	Name *string
}

func (a *YieldPipe) Pipe() (string, error) {
	if a.Name != nil {
		return fmt.Sprintf(`|> yield(name: "%s")`, *a.Name), nil
	}
	return "|> yield()", nil
}
