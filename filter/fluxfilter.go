package filter

import (
	"fmt"
	"strings"
)

type FluxFilter struct {
	Not *FluxFilter
	Or  []*FluxFilter
	And []*FluxFilter

	Measurement       *string
	MeasurementNEQ    *string
	MeasurementMatch  *string
	MeasurementNMatch *string

	Field       *string
	FieldNEQ    *string
	FieldMatch  *string
	FieldNMatch *string

	TagKey    *string
	Tag       *string
	TagNEQ    *string
	TagMatch  *string
	TagNMatch *string

	Value *string
}

func (f *FluxFilter) AddNot(n *FluxFilter) {
	f.Not = n
}

func (f *FluxFilter) AddOr(n *FluxFilter) {
	f.Or = append(f.Or, n)
}

func (f *FluxFilter) AddAnd(n *FluxFilter) {
	f.And = append(f.And, n)
}

func (f *FluxFilter) p() (string, error) {
	var equations []string

	if f.Not != nil {
		p, err := f.Not.p()
		if err != nil {
			return "", err
		}
		equations = append(equations, fmt.Sprintf("not (%s)", p))
	}

	switch n := len(f.Or); {
	case n == 1:
		p, err := f.Or[0].p()
		if err != nil {
			return "", err
		}
		equations = append(equations, p)
	case n > 1:
		or := make([]string, 0, n)
		for _, w := range f.Or {
			p, err := w.p()
			if err != nil {
				return "", err
			}
			or = append(or, p)
		}
		equations = append(equations, fmt.Sprintf("(%s)", strings.Join(or, " or ")))
	}

	switch n := len(f.And); {
	case n == 1:
		p, err := f.And[0].p()
		if err != nil {
			return "", err
		}
		equations = append(equations, p)
	case n > 1:
		and := make([]string, 0, n)
		for _, w := range f.And {
			p, err := w.p()
			if err != nil {
				return "", err
			}
			and = append(and, p)
		}
		equations = append(equations, fmt.Sprintf("(%s)", strings.Join(and, " and ")))
	}

	if f.Measurement != nil {
		equations = append(equations, fmt.Sprintf("r._measurement == \"%s\"", *f.Measurement))
	}

	if f.MeasurementNEQ != nil {
		equations = append(equations, fmt.Sprintf("r._measurement != \"%s\"", *f.MeasurementNEQ))
	}

	if f.MeasurementMatch != nil {
		equations = append(equations, fmt.Sprintf("r._measurement =~ \"%s\"", *f.MeasurementMatch))
	}

	if f.MeasurementNMatch != nil {
		equations = append(equations, fmt.Sprintf("r._measurement !~ \"%s\"", *f.MeasurementNMatch))
	}

	if f.Field != nil {
		equations = append(equations, fmt.Sprintf("r._field == \"%s\"", *f.Field))
	}

	if f.FieldNEQ != nil {
		equations = append(equations, fmt.Sprintf("r._field != \"%s\"", *f.FieldNEQ))
	}

	if f.FieldMatch != nil {
		equations = append(equations, fmt.Sprintf("r._field =~ \"%s\"", *f.FieldMatch))
	}

	if f.FieldNMatch != nil {
		equations = append(equations, fmt.Sprintf("r._field !~ \"%s\"", *f.FieldNMatch))
	}

	if f.TagKey != nil {
		if f.Tag != nil {
			equations = append(equations, fmt.Sprintf("r.%s == \"%s\"", *f.TagKey, *f.Tag))
		}
		if f.TagNEQ != nil {
			equations = append(equations, fmt.Sprintf("r.%s != \"%s\"", *f.TagKey, *f.TagNEQ))
		}
		if f.TagMatch != nil {
			equations = append(equations, fmt.Sprintf("r.%s =~ \"%s\"", *f.TagKey, *f.TagMatch))
		}
		if f.TagNMatch != nil {
			equations = append(equations, fmt.Sprintf("r.%s !~ \"%s\"", *f.TagKey, *f.TagNMatch))
		}
	}

	if f.Value != nil {
		equations = append(equations, fmt.Sprintf("r._value %s", *f.Value))
	}

	switch len(equations) {
	case 0:
		return "", fmt.Errorf("empty predicate FluxFilter")
	case 1:
		return equations[0], nil
	default:
		return strings.Join(equations, " and "), nil
	}
}

func (f *FluxFilter) Pipe() (string, error) {
	p, err := f.p()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`|> filter(fn: (r) => %s)`, p), nil
}
