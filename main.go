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
	s3logclient := s3logs.New(c.Region, c.Bucket, c.Prefix, c.Concurrency)
	lokiclient := loki.New(c.LokiHost)

	for {
		cflogs, nextfile, err := s3logclient.Download()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}

		err = lokiclient.PushLogs(cflogs, c.LokiLabels, c.Prefix)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		if nextfile == "" {
			break
		}
	}
}
