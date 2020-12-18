package loki

import (
	"io"
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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		response = string(b)
	}))

	defer ts.Close()
	loki := New(ts.URL[7:])
	logs := []*cflog.CFLog{mockCFLog("bogus-file1"), mockCFLog("bogus-file2")}
	err := loki.PushLogs(logs, "{\"foo\": \"bar\"}")
	require.NoError(t, err)
	require.Contains(t, response, "foo")
}

func TestGetLatestLog(t *testing.T) {
	respstr := "{\"data\":{ \"result\": [ { \"values\": [[\"1\",\"Info: { \\\"Filename\\\": \\\"bogus-testfile\\\"}\"]]}]}}"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, respstr)
		_, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
	}))
	loki := New(ts.URL[7:])
	filename, err := loki.GetLatestLog("{source=\"cloudfront\",job=\"cloudfront2loki\"}")
	require.NoError(t, err)
	require.Equal(t, filename, "bogus-testfile")
}

func mockCFLog(filename string) (log *cflog.CFLog) {
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
		X_edge_response_result_type: "-",
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
