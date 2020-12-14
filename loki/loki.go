package loki

import(
	"fmt"
	"time"

	"github.com/afiskon/promtail-client/promtail"
)

type Loki struct {
	LokiHost		string
}

type log interface {}

func New(lokihost string) (loki *Loki){
	loki = &Loki{
		LokiHost: lokihost,
	}
	return
}

func (l *Loki) PushLogs(logrecords []string, labels string, source string) (err error) {
	pushurl := fmt.Sprintf("http://%s/api/prom/push",l.LokiHost)
	conf := promtail.ClientConfig{
		PushURL:            pushurl,
		Labels:             labels,
		BatchWait:          5 * time.Second,
		BatchEntriesNumber: 10000,
		SendLevel: 			promtail.INFO,
		PrintLevel: 		promtail.DEBUG,
	}
	lokiclient, err := promtail.NewClientProto(conf)
	if err != nil {
		return
	}	
	for _,log := range logrecords {
		lokiclient.Infof("%s\n",log)
	}
	lokiclient.Shutdown()	
	return
}