package main

import (
	"fmt"
	"os"
	"strings"
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
	const timeformat = "2006-01-02-15"   // Date isn't, random these numbers are the format
	loc, err := time.LoadLocation("UTC") // Assume UTC
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
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

	labelfields := []string{"Filename", "Date", "X_edge_result_type"}
	lokiclient := loki.New(
		c.LokiHost,
		c.LokiBatchSize,
		c.LokiBatchWaitSeconds,
		c.LokiLabels,
		labelfields,
	)
	s3logclient := s3logs.New(c.Region, c.Bucket, c.Prefix, c.Concurrency)

	// File the last file pushed to Loki
	nextfile, err := lokiclient.GetLatestLog(c.LokiLabels)
	if err != nil {
		log.Error(fmt.Sprintf("%v", err))
	}
	if nextfile == "" || c.IgnoreLokiLatestLog {
		nextfile = c.StartAfterFile
	}
	log.Infof("Last logfile in loki found: %s", nextfile)
	log.Infof("Starting with %s", nextfile)

	// Trim off date from the last file so we can start at the top of the hour
	datearray := strings.Split(strings.TrimPrefix(nextfile, c.Prefix), ".")
	lastfiledate := "2020-12-29-00" // This is a random date
	if len(datearray) > 1 {
		lastfiledate = datearray[1]
	}
	// Time of last file pushed to Loki
	t, err := time.Parse(timeformat, lastfiledate)
	if err != nil {
		log.Errorf(fmt.Sprintf("%v", err))
		os.Exit(1)
	}
	// For keeping track of files we've pushed to Loki
	pushedfiles := map[string]int{}

	log.Warn("Starting Loop")
	for {
		timediff := time.Now().In(loc).Sub(t).Minutes()
		// If the nextfile within 2 hours of now? If so, watch the bucket, if not bulk download
		if nextfile == "" && timediff < 120 {
			thishour := time.Now().In(loc).Format(timeformat)
			lasthour := time.Now().In(loc).Add(time.Duration(-60) * time.Minute).Format(timeformat)
			for _, hour := range []string{lasthour, thishour} {
				log.Infof("Watching s3 prefix %s...", c.Prefix+"-"+hour)
				cflogs, pushedfiles, err := s3logclient.WatchBucket(c.Prefix+"."+hour, pushedfiles)
				if err != nil {
					log.Errorf(fmt.Sprintf("%v", err))
				}
				if len(cflogs) > 0 {
					log.Info("Pushing files up to Loki...")
					err = lokiclient.PushLogs(cflogs)
					if err != nil {
						log.Errorf(fmt.Sprintf("%v", err))
					} else {
						for _, file := range cflogs {
							pushedfiles[file.Filename] = 1
						}
					}
				}
			}
			log.Info(fmt.Sprintf("Sleeping for %v seconds ", c.LoopSleepSeconds))
			time.Sleep(time.Duration(c.LoopSleepSeconds * int(time.Second)))
		} else {
			log.Info("Bulk downloading...")
			log.Debugf("Reading files after %s, %s", c.Prefix+"."+lastfiledate, nextfile)
			s3logclient = s3logs.New(c.Region, c.Bucket, c.Prefix+"."+lastfiledate, c.Concurrency)
			var cflogs []*cflog.CFLog
			cflogs, nextfile, err = s3logclient.Download(nextfile)
			if err != nil {
				log.Errorf(fmt.Sprintf("%v", err))
			}
			t = t.Add(time.Duration(61) * time.Minute)
			lastfiledate = t.Format(timeformat)
			if len(cflogs) > 0 {
				log.Info("Pushing log lines to Loki...")
				err = lokiclient.PushLogs(cflogs)
				if err != nil {
					log.Errorf(fmt.Sprintf("%v", err))
				}
				// clear out previous pushed files and add the most recent for the switch over to watching a bucket
				pushedfiles = map[string]int{}
				for _, file := range cflogs {
					pushedfiles[file.Filename] = 1
				}
			}
		}
	}
}
