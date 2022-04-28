package client

import "time"

type ResutTable struct {
	Measurement string
	Field       string
	Result      string
	Start       time.Time
	Stop        time.Time
	Table       int
	Time        time.Time
	Value       interface{}
	Values      map[string]interface{}
}
