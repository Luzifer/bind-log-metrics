package main

import (
	"sync"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
	"github.com/pkg/errors"
)

const (
	influxChunkSize     = 1000
	influxPointExpiry   = 10 * time.Minute // If the point isn't submitted in this time, drop it
	influxWriteInterval = 10 * time.Second
)

type metricsSender struct {
	batch     influx.BatchPoints
	batchLock sync.Mutex
	client    influx.Client
	database  string
	errs      chan error
}

func newMetricsSender(host, user, pass, database string) (*metricsSender, error) {
	out := &metricsSender{
		database: database,
		errs:     make(chan error, 10),
	}
	return out, out.initialize(host, user, pass)
}

func (m *metricsSender) Errors() <-chan error {
	return m.errs
}

func (m *metricsSender) RecordPoint(name string, tags map[string]string, fields map[string]interface{}) error {
	pt, err := influx.NewPoint(name, tags, fields, time.Now())
	if err != nil {
		return err
	}

	m.batchLock.Lock()
	defer m.batchLock.Unlock()
	m.batch.AddPoint(pt)

	return nil
}

func (m *metricsSender) filterExpiredPoints(pts []*influx.Point) []*influx.Point {
	var out []*influx.Point

	for _, pt := range pts {
		if time.Since(pt.Time()) < influxPointExpiry {
			out = append(out, pt)
		}
	}

	return out
}

func (m *metricsSender) resetBatch() error {
	b, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database: m.database,
	})
	if err != nil {
		return err
	}

	m.batch = b
	return nil
}

func (m *metricsSender) sendLoop() {
	for range time.Tick(influxWriteInterval) {

		m.batchLock.Lock()

		failedBatch, err := influx.NewBatchPoints(influx.BatchPointsConfig{
			Database: cfg.InfluxDBName,
		})
		if err != nil {
			m.errs <- errors.Wrap(err, "creating batchpoints")
			continue
		}

		for i := 0; i < len(m.batch.Points()); i += influxChunkSize {
			chunk, err := influx.NewBatchPoints(influx.BatchPointsConfig{
				Database: cfg.InfluxDBName,
			})
			if err != nil {
				m.errs <- errors.Wrap(err, "creating batchpoints")
				continue
			}

			end := i + influxChunkSize
			if end > len(m.batch.Points()) {
				end = len(m.batch.Points())
			}

			chunk.AddPoints(m.batch.Points()[i:end])

			if err := m.client.Write(chunk); err != nil {
				m.errs <- err
				failedBatch.AddPoints(m.filterExpiredPoints(m.batch.Points()[i:end]))
				continue
			}
		}

		m.batch = failedBatch
		m.batchLock.Unlock()
	}
}

func (m *metricsSender) initialize(host, user, pass string) error {
	influxClient, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     host,
		Username: user,
		Password: pass,
		Timeout:  2 * time.Second,
	})
	if err != nil {
		return err
	}

	m.client = influxClient
	if err := m.resetBatch(); err != nil {
		return err
	}
	go m.sendLoop()

	return nil
}
