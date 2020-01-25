package main

import (
	"sync"
	"time"

	influx "github.com/influxdata/influxdb1-client/v2"
)

const (
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
		if err := m.client.Write(m.batch); err != nil {
			m.errs <- err
			m.batchLock.Unlock()
			continue
		}
		m.resetBatch()
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
