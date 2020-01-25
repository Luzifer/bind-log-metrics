package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

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

func init() {
	rconfig.AutoEnv(true)
	if err := rconfig.ParseAndValidate(&cfg); err != nil {
		log.Fatalf("Unable to parse commandline options: %s", err)
	}

	if cfg.VersionAndExit {
		fmt.Printf("bind-log-metrics %s\n", version)
		os.Exit(0)
	}

	if l, err := log.ParseLevel(cfg.LogLevel); err != nil {
		log.WithError(err).Fatal("Unable to parse log level")
	} else {
		log.SetLevel(l)
	}
}

func main() {
	var err error

	if metrics, err = newMetricsSender(cfg.InfluxHost, cfg.InfluxUser, cfg.InfluxPass, cfg.InfluxDBName); err != nil {
		log.WithError(err).Fatal("Unable to create metrics client")
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var line = scanner.Text()

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
			log.WithError(err).Error("Unable to process line")
		}
	}

	if err := scanner.Err(); err != nil {
		log.WithError(err).Fatal("Scanner caused an error")
	}
}

func handleRecord(m metric, groups []string) error {
	return errors.Wrap(
		metrics.RecordPoint(string(m), map[string]string{
			"client": groups[1],
			"domain": groups[2],
			"type":   groups[3],
		}, map[string]interface{}{
			"count": 1,
		}),
		"Unable to add metrics point",
	)
}
