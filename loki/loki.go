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
)

type Loki struct {
	LokiHost string
}

func New(lokihost string) (loki *Loki) {
	loki = &Loki{
		LokiHost: lokihost,
	}
	return
}

func (l *Loki) PushLogs(logrecords []*cflog.CFLog, labels string) (err error) {
	pushurl := fmt.Sprintf("http://%s/api/prom/push", l.LokiHost)
	conf := promtail.ClientConfig{
		PushURL:            pushurl,
		Labels:             labels,
		BatchWait:          5 * time.Second,
		BatchEntriesNumber: 5000,
		SendLevel:          promtail.INFO,
		PrintLevel:         promtail.DEBUG,
	}
	lokiclient, err := promtail.NewClientProto(conf)
	if err != nil {
		return
	}
	for _, log := range logrecords {
		var jsondata []byte
		jsondata, err = json.Marshal(log)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		jsonstr := string(jsondata)
		switch log.X_edge_response_result_type {
		case "Hit":
			lokiclient.Infof("%s\n", jsonstr)
		case "Miss":
			lokiclient.Infof("%s\n", jsonstr)
		case "RefreshHit":
			lokiclient.Infof("%s\n", jsonstr)
		case "Redirect":
			lokiclient.Infof("%s\n", jsonstr)
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
	err = json.Unmarshal(body, &jsondata)
	if err != nil {
		return latestlog, err
	}

	// The result isn't true json, it has a leading tag which is either 'Info: or 'Error'
	if len(jsondata.Data.Result) < 1 {
		return "", nil
	}
	d := strings.SplitN(jsondata.Data.Result[0].Values[0][1], ":", 2)
	if len(d) < 2 {
		return "", nil
	}
	logdata := d[1]
	var jsonlog struct{ Filename string }
	err = json.Unmarshal([]byte(logdata), &jsonlog)
	if err != nil {
		return latestlog, err
	}
	latestlog = jsonlog.Filename
	return
}
