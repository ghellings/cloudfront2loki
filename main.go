package main

import (
	"fmt"

	"github.com/ghellings/cloudfront2loki/config"
	"github.com/ghellings/cloudfront2loki/loki"
	"github.com/ghellings/cloudfront2loki/s3"
)

func main() {
	c, err := config.LoadConfig(".")
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	fmt.Printf("%+v\n", c)
	s3logclient := s3logs.New(c.Region, c.Bucket, c.Prefix, c.Concurrency)
	lokiclient := loki.New(c.LokiHost)

	nextfile, err := lokiclient.GetLatestLog(c.LokiLabels)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
	if nextfile == "" {
		nextfile = c.StartAfterFile
	}
	for {
		cflogs, nextfile, err := s3logclient.Download(nextfile)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}

		err = lokiclient.PushLogs(cflogs, c.LokiLabels)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		if nextfile == "" {
			break
		}
	}
}
