package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/ghellings/cloudfront2loki/config"
	"github.com/ghellings/cloudfront2loki/loki"
	"github.com/ghellings/cloudfront2loki/s3"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

func main() {
	c, err := config.LoadConfig(".")
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	switch c.LogLevel {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	s3logclient := s3logs.New(c.Region, c.Bucket, c.Prefix, c.Concurrency)
	lokiclient := loki.New(c.LokiHost, c.LokiLogLevel, c.LokiBatchSize, c.LokiBatchWaitSeconds)

	nextfile, err := lokiclient.GetLatestLog(c.LokiLabels)
	if err != nil {
		log.Error(fmt.Sprintf("%v", err))
	}
	if nextfile == "" || c.IgnoreLokiLatestLog {
		nextfile = c.StartAfterFile
	}

	switch {
	case c.Once:
		log.Warn("Running Once")
		for {
			cflogs, nextfile, err := s3logclient.Download(nextfile)
			if err != nil {
				log.Error(fmt.Sprintf("%v", err))
			}
			err = lokiclient.PushLogs(cflogs, c.LokiLabels)
			if err != nil {
				log.Error(fmt.Sprintf("%v", err))
			}
			if nextfile == "" {
				break
			}
		}
		return
	default:
		log.Warn("Starting Loop")
		for {
			log.Info("Reading files after " + nextfile)
			var cflogs []*cflog.CFLog
			cflogs, nextfile, err = s3logclient.Download(nextfile)
			if err != nil {
				log.Error(fmt.Sprintf("%v", err))
			}
			log.Info("Pushing files up to " + nextfile)
			err = lokiclient.PushLogs(cflogs, c.LokiLabels)
			if err != nil {
				log.Error(fmt.Sprintf("%v", err))
			}
			if nextfile == "" {
				log.Info(fmt.Sprintf("Sleeping for %v seconds ", c.LoopSleepSeconds))
				time.Sleep(time.Duration(c.LoopSleepSeconds * int(time.Second)))
				nextfile, err = lokiclient.GetLatestLog(c.LokiLabels)
				if err != nil {
					log.Error(fmt.Sprintf("%v", err))
				}
			}
		}
	}
}
