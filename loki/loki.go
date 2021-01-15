package loki

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/sirupsen/logrus"
)

type Loki struct {
	LokiHost           string
	BaseLabels         string
	LabelFields        []string
	BatchEntriesNumber int
	BatchWaitSeconds   time.Duration
	entries            chan LabeledEntry
	quit               chan struct{}
	waitGroup          sync.WaitGroup
}

type LabeledEntry struct {
	entry  logproto.Entry
	labels string
}

func New(lokihost string, args ...interface{}) (loki *Loki) {
	batch := 500
	if len(args) > 0 {
		batch = args[0].(int)
	}
	batchwait := 5 * time.Second
	if len(args) > 1 {
		batchwait = time.Duration(args[1].(int)) * time.Second
	}
	baselabels := "{}"
	if len(args) > 2 {
		baselabels = args[2].(string)
	}
	labelfields := []string{}
	if len(args) > 3 {
		labelfields = args[3].([]string)
	}
	loki = &Loki{
		LokiHost:           lokihost,
		BatchEntriesNumber: batch,
		BatchWaitSeconds:   batchwait,
		entries:            make(chan LabeledEntry),
		quit:               make(chan struct{}),
		BaseLabels:         baselabels,
		LabelFields:        labelfields,
	}
	go loki.run()
	return
}

func (l *Loki) PushLogs(logrecords []*cflog.CFLog) (err error) {
	filename := ""
	skippedfilename := ""

	// Parse log lines
	for _, log := range logrecords {
		// Turn log line into json
		var jsondata []byte
		jsondata, err = json.Marshal(log)
		if err != nil {
			return
		}
		jsonstr := string(jsondata)
		// Skip this log line if we're already figured out it's in loki
		if log.Filename == skippedfilename {
			continue
		}
		// This log line came from a file that's different than the last
		if log.Filename != filename {
			// Check if this file is already in loki
			var exists bool
			if exists, err = l.IsLogInLoki(log.Filename); exists {
				if log.Filename != skippedfilename {
					logrus.Warnf("Skipping file %s, already in Loki", log.Filename)
					skippedfilename = log.Filename
				}
				continue
			}
			err = l.protoEntry(*log, jsonstr)
			if err != nil {
				return
			}
			filename = log.Filename
			logrus.Debugf("Created a new Loki label for %s", filename)
		}
	}
	return
}

func (l *Loki) newLabels(log cflog.CFLog) (newlabels string) {
	newlabels = strings.TrimRight(l.BaseLabels, "}")
	for _, field := range l.LabelFields {
		val := reflect.ValueOf(log).FieldByName(field)
		newlabels = fmt.Sprintf("%s,%s=\"%s\"", newlabels, field, val)
	}
	newlabels = fmt.Sprintf("%s}", newlabels)
	return
}

func (l *Loki) protoEntry(log cflog.CFLog, jsonstr string) (err error) {
	var t time.Time
	t, err = time.Parse(time.RFC3339, log.Date+"T"+log.Time+"Z")
	if err != nil {
		return
	}
	labels := l.newLabels(log)
	l.entries <- LabeledEntry{
		entry: logproto.Entry{
			Timestamp: t,
			Line:      jsonstr,
		},
		labels: labels,
	}
	return
}

func (l *Loki) send(labeledentries []LabeledEntry) (err error) {
	mappedentries := make(map[string][]logproto.Entry)
	for _, log := range labeledentries {
		mappedentries[log.labels] = append(mappedentries[log.labels], log.entry)
	}
	var streams []logproto.Stream
	for label, entries := range mappedentries {
		streams = append(streams, logproto.Stream{
			Labels:  label,
			Entries: entries,
		})
	}
	pushreq := logproto.PushRequest{
		Streams: streams,
	}
	buf, err := proto.Marshal(&pushreq)
	if err != nil {
		logrus.Errorf("Failed to marshal streams: %v", err)
		return err
	}
	buf = snappy.Encode(nil, buf)
	pushurl := fmt.Sprintf("http://%s/api/prom/push", l.LokiHost)
	client := &http.Client{}
	req, err := http.NewRequest("POST", pushurl, bytes.NewBuffer(buf))
	if err != nil {
		logrus.Errorf("Failed to POST to Loki host %s :%v", l.LokiHost, err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		body, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("Unexpected HTTP status code: %d url: %s message: %s", resp.StatusCode, pushurl, string(body))
		return err
	}
	return
}

func (l *Loki) run() {
	var batch []LabeledEntry
	batchSize := 0
	maxWait := time.NewTimer(l.BatchWaitSeconds)

	defer func() {
		if batchSize > 0 {
			l.send(batch)
		}

		l.waitGroup.Done()
	}()

	for {
		select {
		case <-l.quit:
			return
		case entry := <-l.entries:
			batchSize++
			batch = append(batch, entry)
			if batchSize >= l.BatchEntriesNumber {
				err := l.send(batch)
				if err != nil {
					logrus.Errorf("Unable to batch messages to Loki: %v", err)
				}
				batch = []LabeledEntry{}
				batchSize = 0
			}
		case <-maxWait.C:
			if batchSize > 0 {
				err := l.send(batch)
				if err != nil {
					logrus.Errorf("Unable to batch messages to Loki: %v", err)
				}
				batch = []LabeledEntry{}
				batchSize = 0
			}
			maxWait.Reset(l.BatchWaitSeconds)
		}
	}
}

func (l *Loki) GetLatestLog(query string) (latestlog string, err error) {
	latestlog = ""
	// Asking loki for the last log entry to get it's filename
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://"+l.LokiHost+"/loki/api/v1/query_range", nil)
	if err != nil {
		return
	}
	q := req.URL.Query()
	q.Add("query", query)
	q.Add("limit", "1")
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return latestlog, err
	}

	// Loki gives us a double json encoded response so we gotta convert twice
	var jsondata struct {
		Data struct {
			Result []struct {
				Values [][]string
			}
		}
	}
	if err = json.Unmarshal(body, &jsondata); err != nil {
		return latestlog, err
	}
	// The result isn't json, it has a leading tag which is either 'Info: or 'Error'
	if len(jsondata.Data.Result) < 1 {
		return
	}
	logdata := strings.SplitN(jsondata.Data.Result[0].Values[0][1], ":", 2)
	if len(logdata) < 2 {
		return
	}
	var jsonlog struct{ Filename string }
	if err := json.Unmarshal([]byte(logdata[1]), &jsonlog); err != nil {
		return latestlog, err
	}
	latestlog = jsonlog.Filename
	return
}

func (l *Loki) IsLogInLoki(filename string) (ret bool, err error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://"+l.LokiHost+"/loki/api/v1/query_range", nil)
	if err != nil {
		return false, err
	}
	// Limit the query to the last two hours
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		return false, err
	}
	starttime := time.Now().In(loc).Add(time.Duration(-120)).UnixNano()

	q := req.URL.Query()
	q.Add("query", fmt.Sprintf("{filename=\"%s\"}", filename))
	q.Add("limit", "1")
	q.Add("start", fmt.Sprintf("%d", starttime))
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	var jsondata struct {
		Data struct {
			Stats struct {
				Ingester struct {
					TotalChunksMatched int
				}
			}
		}
	}
	if err = json.Unmarshal(body, &jsondata); err != nil {
		return
	}
	if jsondata.Data.Stats.Ingester.TotalChunksMatched > 0 {
		ret = true
	}
	return
}
