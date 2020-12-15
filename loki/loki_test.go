package loki

import (
	"testing"
	"fmt"

	"github.com/ghellings/cloudfront2loki/cflog"
	"net/http"
	"net/http/httptest"
)

func TestNew(t *testing.T) {
	loki := New("bogus")
	if loki == nil {
		t.Error("Expected loki to not be nil\n")
	}
}

func TestPushLogs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, client")
		fmt.Println(r)
	}))

	defer ts.Close()
	loki := New(ts.URL)
	logs := []*cflog.CFLog{}
	err := loki.PushLogs(logs,"{\"foo\":\"bar\"}", "bogus")
	if err != nil {
		t.Errorf("Expected no err, got: %s\n",err)
	}

}