package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/sirupsen/logrus"

	"github.com/Luzifer/rconfig/v2"
)

type metric string

const (
	metricQuery metric = "dns_query"
	metricBlock metric = "dns_block"
)

var (
	cfg = struct {
		InfluxDBName   string `flag:"influx-db-name" description:"Database name of the InfluxDB" validate:"nonzero"`
		InfluxHost     string `flag:"influx-host" description:"Hostname of the InfluxDB" validate:"nonzero"`
		InfluxPass     string `flag:"influx-pass" description:"Password of the InfluxDB" validate:"nonzero"`
		InfluxUser     string `flag:"influx-user" description:"Username of the InfluxDB" validate:"nonzero"`
		LogLevel       string `flag:"log-level" default:"info" description:"Log level (debug, info, warn, error, fatal)"`
		VersionAndExit bool   `flag:"version" default:"false" description:"Prints current version and exits"`
	}{}

	metrics *metricsSender

	regBlock = regexp.MustCompile(`([0-9:.]+)#.*: rpz QNAME NXDOMAIN rewrite ([^/]+)/([^/]+)/IN`)
	regQuery = regexp.MustCompile(`([0-9:.]+)#.*: query: ([^ ]+) IN ([^ ]+) \+`)

	version = "dev"
)

func initApp() error {
	rconfig.AutoEnv(true)
	if err := rconfig.ParseAndValidate(&cfg); err != nil {
		return fmt.Errorf("parsing CLI options: %w", err)
	}

	l, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("parsing log-level: %w", err)
	}
	logrus.SetLevel(l)

	return nil
}

func main() {
	var err error

	if err = initApp(); err != nil {
		logrus.WithError(err).Fatal("initializing app")
	}

	if cfg.VersionAndExit {
		fmt.Printf("bind-log-metrics %s\n", version)
		os.Exit(0)
	}

	if metrics, err = newMetricsSender(cfg.InfluxHost, cfg.InfluxUser, cfg.InfluxPass, cfg.InfluxDBName); err != nil {
		logrus.WithError(err).Fatal("creating metrics client")
	}

	go func() {
		for err := range metrics.Errors() {
			logrus.WithError(err).Error("metrics processing caused an error")
		}
	}()

	var input io.Reader = os.Stdin
	if len(rconfig.Args()) > 1 {
		f, err := os.Open(rconfig.Args()[1])
		if err != nil {
			logrus.WithError(err).Fatal("opening input file")
		}
		defer f.Close()

		input = f
	}

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()

		// Re-yield the line scanned from stdin
		fmt.Fprintln(os.Stdout, line)

		switch {

		case regBlock.MatchString(line):
			err = handleRecord(metricBlock, regBlock.FindStringSubmatch(line))

		case regQuery.MatchString(line):
			err = handleRecord(metricQuery, regQuery.FindStringSubmatch(line))

		default:
			continue

		}

		if err != nil {
			logrus.WithError(err).Error("processing line")
		}
	}

	if err := scanner.Err(); err != nil {
		logrus.WithError(err).Fatal("Scanner caused an error")
	}
}

func handleRecord(m metric, groups []string) (err error) {
	if err = metrics.RecordPoint(string(m), map[string]string{
		"client": groups[1],
		"domain": groups[2],
		"type":   groups[3],
	}, map[string]any{
		"count": 1,
	}); err != nil {
		return fmt.Errorf("adding metrics point: %w", err)
	}

	return nil
}
