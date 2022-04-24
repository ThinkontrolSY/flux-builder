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

type LastPipe struct{}

func (a *LastPipe) Pipe() (string, error) {
	return "|> last()", nil
}
