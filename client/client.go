package client

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/ThinkontrolSY/flux-builder/query"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	iq "github.com/influxdata/influxdb-client-go/v2/api/query"
	"github.com/influxdata/influxdb-client-go/v2/domain"
)

type Config struct {
	Uri   string `mapstructure:"uri"`
	Token string `mapstructure:"token"`
	Org   string `mapstructure:"org"`
}

type MeasurementSchema struct {
	Measurement string   `json:"measurement"`
	Fields      []string `json:"fields"`
	Tags        []string `json:"tags"`
}

type InfluxClient struct {
	client influxdb2.Client
	org    string
}

func NewClient(config Config) (*InfluxClient, func()) {
	influxClient := influxdb2.NewClient(config.Uri, config.Token)
	return &InfluxClient{
		client: influxClient,
		org:    config.Org,
	}, influxClient.Close
}

func (w *InfluxClient) GetOrg() string {
	return w.org
}

func (w *InfluxClient) CreateBucket(ctx context.Context, bucket string, retention int64) error {
	bucketApi := w.client.BucketsAPI()
	// 获取组织 API
	orgAPI := w.client.OrganizationsAPI()

	// 获取你的组织
	org, err := orgAPI.FindOrganizationByName(ctx, w.org)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = bucketApi.CreateBucketWithName(ctx, org, bucket, domain.RetentionRule{
		EverySeconds: retention,
	})
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (w *InfluxClient) Buckets(ctx context.Context) ([]string, error) {
	var buckets []string
	bucketApi := w.client.BucketsAPI()
	domainBuckets, err := bucketApi.FindBucketsByOrgName(ctx, w.org)
	if err != nil {
		return nil, err
	}
	if domainBuckets != nil {
		for _, b := range *domainBuckets {
			buckets = append(buckets, b.Name)
		}
	}
	return buckets, nil
}

func (w *InfluxClient) Schema(ctx context.Context, bucket string) ([]*MeasurementSchema, error) {
	var schema []*MeasurementSchema
	queryAPI := w.client.QueryAPI(w.org)
	result, err := queryAPI.Query(ctx, fmt.Sprintf(`import "influxdata/influxdb/schema"
	schema.measurements(bucket: "%s")`, bucket))
	if err != nil {
		log.Warn("query error: %v", err)
		return nil, err
	}
	if result.Err() != nil {
		log.Warn("query parsing error: %s", result.Err().Error())
		return nil, result.Err()
	}
	for result.Next() {
		measurement := fmt.Sprintf("%s", result.Record().Value())
		fieldResult, fieldErr := queryAPI.Query(ctx, fmt.Sprintf(`import "influxdata/influxdb/schema"
		schema.measurementFieldKeys(bucket: "%s", measurement: "%s",)`, bucket, measurement))
		if fieldErr != nil {
			log.Warn("query error: %v", fieldErr)
			return nil, fieldErr
		}
		if fieldResult.Err() != nil {
			log.Warn("query parsing error: %s", fieldResult.Err().Error())
			return nil, fieldResult.Err()
		}
		var fields []string
		for fieldResult.Next() {
			fields = append(fields, fmt.Sprintf("%s", fieldResult.Record().Value()))
		}
		tagResult, tagErr := queryAPI.Query(ctx, fmt.Sprintf(`import "influxdata/influxdb/schema"
		schema.measurementTagKeys(bucket: "%s", measurement: "%s",)`, bucket, measurement))
		if tagErr != nil {
			log.Warn("query error: %v", tagErr)
			return nil, tagErr
		}
		if tagResult.Err() != nil {
			log.Warn("query parsing error: %s", tagResult.Err().Error())
			return nil, tagResult.Err()
		}
		var tags []string
		for tagResult.Next() {
			tag := fmt.Sprintf("%s", tagResult.Record().Value())
			if !strings.HasPrefix(tag, "_") {
				tags = append(tags, fmt.Sprintf("%s", tag))
			}
		}

		schema = append(schema, &MeasurementSchema{
			Measurement: measurement,
			Fields:      fields,
			Tags:        tags,
		})
	}
	return schema, nil
}

func (w *InfluxClient) TagValues(ctx context.Context, bucket, measurement, tag string) ([]string, error) {
	var tags []string
	queryAPI := w.client.QueryAPI(w.org)
	result, err := queryAPI.Query(ctx, fmt.Sprintf(`import "influxdata/influxdb/schema"
	schema.measurementTagValues(
		bucket: "%s",
		tag: "%s",
		measurement: "%s",
	)`, bucket, tag, measurement))
	if err != nil {
		log.Warn("query error: %v", err)
		return nil, err
	}
	if result.Err() != nil {
		log.Warn("query parsing error: %s", result.Err().Error())
		return nil, result.Err()
	}
	for result.Next() {
		tags = append(tags, fmt.Sprintf("%s", result.Record().Value()))
	}
	return tags, nil
}

func (w *InfluxClient) Query(ctx context.Context, q query.FluxQuery) ([]*iq.FluxRecord, error) {
	flux, err := q.QueryString()
	if err != nil {
		return nil, err
	}
	queryAPI := w.client.QueryAPI(w.org)
	result, err := queryAPI.Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	var tables []*iq.FluxRecord
	// check for an error
	if result.Err() != nil {
		log.Warn("query parsing error: %s", result.Err().Error())
	}
	for result.Next() {
		// Access data
		record := result.Record()
		// if result.TableChanged() {
		// 	log.Info("table: %s", result.TableMetadata().String())
		// 	// log.Debugf("table: %v, field: %v, time: %v, value: %v", record.Table(), record.Field(), record.Time(), record.Value())
		// }

		tables = append(tables, record)
		// tables = append(tables, ResutTable{
		// 	Measurement: record.Measurement(),
		// 	Field:       record.Field(),
		// 	Result:      record.Result(),
		// 	Start:       record.Start(),
		// 	Stop:        record.Stop(),
		// 	Time:        record.Time(),
		// 	Table:       record.Table(),
		// 	Value:       record.Value(),
		// 	Values:      record.Values(),
		// })

	}
	return tables, nil
}

func (w *InfluxClient) StrQuery(ctx context.Context, q string) ([]*iq.FluxRecord, error) {
	queryAPI := w.client.QueryAPI(w.org)
	result, err := queryAPI.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	var tables []*iq.FluxRecord
	// check for an error
	if result.Err() != nil {
		log.Warn("query parsing error: %s", result.Err().Error())
	}
	for result.Next() {
		if result.TableChanged() {
			log.Warn("table: %s", result.TableMetadata().String())
		}
		tables = append(tables, result.Record())
	}
	return tables, nil
}

func (w *InfluxClient) QueryRaw(ctx context.Context, q query.FluxQuery) (string, error) {
	flux, err := q.QueryString()
	if err != nil {
		return "", err
	}
	queryAPI := w.client.QueryAPI(w.org)
	result, err := queryAPI.QueryRaw(ctx, flux, influxdb2.DefaultDialect())
	if err != nil {
		return "", err
	}
	return result, nil
}

func (w *InfluxClient) WriteAPI(bucket string) api.WriteAPI {
	return w.client.WriteAPI(w.org, bucket)
}
