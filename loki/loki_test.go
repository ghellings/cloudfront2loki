package loki

import (
	"testing"
)

func TestNew(t *testing.T) {
	loki := New("bogus")
	if loki == nil {
		t.Error("Expected loki to not be nil\n")
	}
}

func TestPushLogs(t *testing.T) {
	loki := New("bogus")
	logs := []string{ "test" }
	err := loki.PushLogs(logs,"{\"foo\":\"bar\"}", "bogus")
	if err != nil {
		t.Errorf("Expected no err, got: %s\n",err)
	}
}