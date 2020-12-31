package loki

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	loki := New("bogus")
	require.NotNil(t, loki)
	loki = New("bogus", "DEBUG")
	require.NotNil(t, loki)
	loki = New("bogus", "INFO")
	require.NotNil(t, loki)
	loki = New("bogus", "WARN")
	require.NotNil(t, loki)
	loki = New("bogus", "DISABLE")
	require.NotNil(t, loki)
	loki = New("bogus", "ERROR", 1000, 500)
	require.NotNil(t, loki)
}

func TestPushLogs(t *testing.T) {
	response := ""
	respstr := "{\"data\":{\"stats\": {\"ingester\":{\"totalChunksMatched\":0}}}}\n"
	ts := mockHttpServer(respstr, 200, &response)
	defer ts.Close()

	var err error
	var loki *Loki
	var logs []*cflog.CFLog
	loglevels := []string{"", "DEBUG", "INFO", "WARN", "DISABLE", "ERROR"}
	for _, loglevel := range loglevels {
		loki = New(ts.URL[7:], loglevel, 1000)
		logs = []*cflog.CFLog{
			cflog.MockCFLog("bogus-file1", "Hit"),
			cflog.MockCFLog("bogus-file2", "Miss"),
			cflog.MockCFLog("bogus-file3", "RefreshHit"),
			cflog.MockCFLog("bogus-file3", "AbortedOrigin"),
			cflog.MockCFLog("bogus-file3", "Redirect"),
			cflog.MockCFLog("bogus-file3", "ClientCommError"),
			cflog.MockCFLog("bogus-file3", "ClientHungUpRequest"),
			cflog.MockCFLog("bogus-file3", "InvalidRequest"),
			cflog.MockCFLog("bogus-file2", "Error"),
		}
		err = loki.PushLogs(logs, "{\"foo\": \"bar\"}")
		require.NoError(t, err)
	}
}

func TestIsLogInLoki(t *testing.T) {
	response := ""
	respstr := "{\"data\":{\"stats\": {\"ingester\":{\"totalChunksMatched\":1}}}}\n"
	ts := mockHttpServer(respstr, 200, &response)
	loki := New(ts.URL[7:])
	exists,err := loki.IsLogInLoki("Testlog")
	require.NoError(t,err)
	require.True(t,exists)
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
	loki = New(ts.URL[7:], "DISABLE")
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
