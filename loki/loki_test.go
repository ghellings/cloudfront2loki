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
}

func TestPushLogs(t *testing.T) {
	response := ""
	ts := mockHttpServer("foo", 204, &response)
	defer ts.Close()
	loki := New(ts.URL[7:])
	logs := []*cflog.CFLog{
		mockCFLog("bogus-file1", "Hit"),
		mockCFLog("bogus-file2", "Miss"),
		mockCFLog("bogus-file3", "RefreshHit"),
		mockCFLog("bogus-file3", "Redirect"),
		mockCFLog("bogus-file2", "Error"),
	}
	err := loki.PushLogs(logs, "{\"foo\": \"bar\"}")
	require.NoError(t, err)
	require.Contains(t, response, "foo")
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

func mockCFLog(filename string, response_type string) (log *cflog.CFLog) {
	log = &cflog.CFLog{
		Filename:                    filename,
		Date:                        "-",
		Time:                        "-",
		X_edge_location:             "-",
		Sc_bytes:                    "-",
		C_ip:                        "-",
		Cs_method:                   "-",
		Cs_Host:                     "-",
		Cs_uri_stem:                 "-",
		Sc_status:                   "-",
		Cs_Referer:                  "-",
		Cs_User_Agent:               "-",
		Cs_uri_query:                "-",
		Cs_Cookie:                   "-",
		X_edge_result_type:          "-",
		X_edge_request_id:           "-",
		X_host_header:               "-",
		Cs_protocol:                 "-",
		Cs_bytes:                    "-",
		Time_taken:                  "-",
		X_forwarded_for:             "-",
		Ssl_protocol:                "-",
		Ssl_cipher:                  "-",
		X_edge_response_result_type: response_type,
		Cs_protocol_version:         "-",
		Fle_status:                  "-",
		Fle_encrypted_fields:        "-",
		C_port:                      "-",
		Time_to_first_byte:          "-",
		X_edge_detailed_result_type: "-",
		Sc_content_type:             "-",
		Sc_content_len:              "-",
		Sc_range_start:              "-",
		Sc_range_end:                "-",
	}
	return
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
