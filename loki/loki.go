package loki

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/afiskon/promtail-client/promtail"
	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/sirupsen/logrus"
)

type Loki struct {
	LokiHost           string
	LogLevel           promtail.LogLevel
	BatchEntriesNumber int
	BatchWaitSeconds   time.Duration
}

func New(lokihost string, args ...interface{}) (loki *Loki) {
	lvl := promtail.ERROR
	if len(args) > 0 {
		switch args[0] {
		case "DEBUG":
			lvl = promtail.DEBUG
		case "INFO":
			lvl = promtail.INFO
		case "WARN":
			lvl = promtail.WARN
		case "DISABLE":
			lvl = promtail.DISABLE
		default:
			lvl = promtail.ERROR
		}
	}
	batch := 500
	if len(args) > 1 {
		batch = args[1].(int)
	}
	batchwait := 5 * time.Second
	if len(args) > 2 {
		batchwait = time.Duration(args[2].(int)) * time.Second
	}
	loki = &Loki{
		LokiHost:           lokihost,
		LogLevel:           lvl,
		BatchEntriesNumber: batch,
		BatchWaitSeconds:   batchwait,
	}
	return
}

func (l *Loki) PushLogs(logrecords []*cflog.CFLog, labels string) (err error) {
	pushurl := fmt.Sprintf("http://%s/api/prom/push", l.LokiHost)
	filename := ""
	skippedfilename := ""
	lokiclient, err := promtail.NewClientProto(promtail.ClientConfig{})
	if err != nil {
		return
	}
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
			if err != nil {
				return
			}
			// Create a new label for this file
			newlabels := fmt.Sprintf(
				"%s,filename=\"%s\"}",
				strings.TrimRight(labels, "}"),
				log.Filename,
			)
			lokiclient, err = promtail.NewClientProto(promtail.ClientConfig{
				PushURL:            pushurl,
				Labels:             newlabels,
				BatchWait:          l.BatchWaitSeconds,
				BatchEntriesNumber: l.BatchEntriesNumber,
				SendLevel:          promtail.INFO,
				PrintLevel:         l.LogLevel,
			})
			if err != nil {
				return
			}
			filename = log.Filename
			logrus.Debugf("Created a new Loki label for %s", filename)
		}
		// Actually log the message to loki
		switch log.X_edge_detailed_result_type {
		case "Hit":
			lokiclient.Infof("%s\n", jsonstr)
		case "Miss":
			lokiclient.Infof("%s\n", jsonstr)
		case "RefreshHit":
			lokiclient.Infof("%s\n", jsonstr)
		case "Redirect":
			lokiclient.Infof("%s\n", jsonstr)
		case "AbortedOrigin":
			lokiclient.Warnf("%s\n", jsonstr)
		case "ClientCommError":
			lokiclient.Warnf("%s\n", jsonstr)
		case "ClientHungUpRequest":
			lokiclient.Warnf("%s\n", jsonstr)
		case "InvalidRequest":
			lokiclient.Warnf("%s\n", jsonstr)
		default:
			lokiclient.Errorf("%s\n", jsonstr)
		}

	}
	lokiclient.Shutdown()
	return
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
