package transformpipe

import (
	"fmt"
	"regexp"
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
