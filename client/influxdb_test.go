package client

import (
	"context"
	"testing"
)

func TestInfluxWrap_Query(t *testing.T) {
	wclient, closeFunc := NewClient(Config{
		Uri:   "https://influxdb.weeforce.com",
		Token: "3bem-Ou9EMDcoI3MXXi6UnnZqVYJBtYjGWz35myYOscZ7H0t6Wdu7ddybSkV8dAJWp4pDB1vwDJ0kxi2Rw_zLw==",
		Org:   "smt",
		// Bucket: "argiculture",
	})
	defer closeFunc()

	// flux := `from(bucket: "argiculture")
	// |> range(start: -1d)
	// |> filter(fn: (r) => r["_measurement"] == "measure-sensor")
	// |> filter(fn: (r) => r["_field"] == "SoilVolumetricWaterContent" or r["_field"] == "SoilTemperature")
	// |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
	// |> yield(name: "mean")`

	buckets, err := wclient.Buckets(context.Background())
	t.Logf("buckets: %+v, err: %v", buckets, err)

	schema, err := wclient.Schema("argiculture")
	if err == nil {
		for _, s := range schema {
			t.Logf("%+v", s)
		}
	} else {
		t.Error(err)
	}

	for _, s := range schema {
		t.Logf("%+v tags:", s.Measurement)
		for _, tag := range s.Tags {
			tagValues, e := wclient.TagValues("argiculture", s.Measurement, tag)
			if e != nil {
				t.Error(e)
			}
			t.Logf("%s: %+v", tag, tagValues)
		}
	}

	// m := "measure-sensor"
	// f1 := "SoilElectricalConductivity"
	// f2 := "SoilTemperature"
	// fluxQuery := query.FluxQuery{
	// 	Bucket: "argiculture",
	// 	Start:  "-1d",
	// 	Filters: []*filter.FluxFilter{
	// 		{
	// 			Measurement: &m,
	// 		},
	// 		{
	// 			Or: []*filter.FluxFilter{
	// 				{
	// 					Field: &f1,
	// 				},
	// 				{
	// 					Field: &f2,
	// 				},
	// 			},
	// 		},
	// 	},
	// 	Transforms: []transformpipe.TransformPipe{
	// 		&transformpipe.AggregatorPipe{
	// 			Every: "1h",
	// 			Fn:    "mean",
	// 		},
	// 	},
	// }

	// tables, err := wclient.Query(fluxQuery)
	// if err != nil {
	// 	t.Error(err)
	// }
	// for _, ta := range tables {
	// 	t.Logf("%+v", ta)
	// }
}
