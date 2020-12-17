package loki

import (
	"io/ioutil"
	"testing"

	"net/http"
	"net/http/httptest"

	"github.com/ghellings/cloudfront2loki/cflog"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	loki := New("bogus")
	require.NotNil(t, loki)
}

func TestPushLogs(t *testing.T) {
	response := ""
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		response = string(b)
	}))

	defer ts.Close()
	loki := New(ts.URL[7:])
	logs := []*cflog.CFLog{
		{Filename: "bob"},
	}
	err := loki.PushLogs(logs, "{\"foo\": \"bar\"}")
	require.NoError(t, err)
	require.Contains(t, response, "foo")
}

func TestGetLatestLog(t *testing.T) {
	loki := New("localhost:3100")
	_, err := loki.GetLatestLog("{source=\"cloudfront\",job=\"cloudfront2loki\"}")
	require.NoError(t, err)
}
