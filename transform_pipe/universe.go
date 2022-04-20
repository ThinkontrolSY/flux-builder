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
