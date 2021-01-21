package loki

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	labels := "{k1=\"v1\",k2=\"v2\"}"
	addfields := []string{"Date", "Filename"}
	loki := New("bogus", 1, 1, labels, addfields)
	require.NotNil(t, loki)
}

func TestPushLogs(t *testing.T) {
	response := ""
	ts := mockHttpServer("", 204, &response)
	defer ts.Close()
	var err error
	var loki *Loki
	var logs []*cflog.CFLog
	loki = New(ts.URL[7:], 1)
	logs = []*cflog.CFLog{
		cflog.MockCFLog("bogus-file1", "Hit", "2021-01-08", "11:50:00"),
		cflog.MockCFLog("bogus-file1", "Hit", "2021-01-08", "11:50:00"),
		cflog.MockCFLog("bogus-file2	", "Hit", "2021-01-08", "11:50:00"),
	}
	err = loki.PushLogs(logs)
	require.NoError(t, err)
}

func TestNewLabels(t *testing.T) {

	labels := "{k1=\"v1\",k2=\"v2\"}"
	addfields := []string{"Date", "Time", "Filename"}
	cflog := cflog.CFLog{Date: "2021-01-08", Time: "11:50:00", Filename: "testfilename"}
	loki := New("bogus", 500, 5, labels, addfields)
	newlabels := loki.newLabels(cflog)
	require.Equal(
		t,
		"{k1=\"v1\",k2=\"v2\",Date=\"2021-01-08\",Time=\"11:50:00\",Filename=\"testfilename\"}",
		newlabels,
	)
}

func TestSend(t *testing.T) {
	response := ""
	timestamp, err := time.Parse(time.RFC3339, "2021-01-08T11:50:00Z")
	require.NoError(t, err)
	entry := logproto.Entry{
		Timestamp: timestamp,
		Line:      "bogus",
	}
	labeledentries := []LabeledEntry{
		{
			entry:  entry,
			labels: "bogus",
		},
	}
	// Expect success
	ts := mockHttpServer("", 204, &response)
	loki := New(ts.URL[7:])
	err = loki.send(labeledentries)
	require.NoError(t, err)

	// Expect Failure
	ts = mockHttpServer("", 500, &response)
	loki = New(ts.URL[7:])
	err = loki.send(labeledentries)
	require.Error(t, err)

}


func TestIsLogInLoki(t *testing.T) {
	response := ""
	respstr := "{\"data\":{\"stats\": {\"ingester\":{\"totalChunksMatched\":1}}}}\n"
	ts := mockHttpServer(respstr, 200, &response)
	loki := New(ts.URL[7:])
	exists, err := loki.IsLogInLoki("Testlog")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGetLatestLog(t *testing.T) {
	// normal log response
	response := ""
	respstr := "{\"data\":{ \"result\": [ { \"values\": [[\"1\",\"Info: { \\\"Filename\\\": \\\"bogus-testfile\\\"}\"]]}]}}"
	ts := mockHttpServer(respstr, 200, &response)
	loki := New(ts.URL[7:])
	filename, err := loki.GetLatestLog("{source=\"cloudfront\",job=\"cloudfront2loki\"}")
	require.NoError(t, err)
	require.Equal(t, filename, "bogus-testfile")
	require.Equal(t, response, "")
	// empty log response
	ts = mockHttpServer("{}", 200, &response)
	loki = New(ts.URL[7:])
	filename, err = loki.GetLatestLog("{source=\"cloudfront\",job=\"cloudfront2loki\"}")
	require.NoError(t, err)
	require.Equal(t, filename, "")
	require.Equal(t, response, "")
}

func mockHttpServer(respstr string, respcode int, resp *string) (ts *httptest.Server) {
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(respcode)
		w.Write([]byte(respstr))
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		*resp = string(b)
	}))
	return
}
