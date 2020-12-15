package loki

import (
	"encoding/json"
	"fmt"
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

func (l *Loki) PushLogs(logrecords []*cflog.CFLog, labels string, source string) (err error) {
	pushurl := fmt.Sprintf("http://%s/api/prom/push", l.LokiHost)
	conf := promtail.ClientConfig{
		PushURL:            pushurl,
		Labels:             labels,
		BatchWait:          5 * time.Second,
		BatchEntriesNumber: 10000,
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
